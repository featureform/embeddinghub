import contextlib
import logging
import sys
from typing import Dict, Optional

import grpc
from dataclasses import dataclass, field
from google.rpc import error_details_pb2, status_pb2

logging.basicConfig(level=logging.DEBUG, filename="grpc_debug.log")


@dataclass
class FFGrpcErrorDetails:
    """
    FFGrpcErrorDetails is a dataclass that represents the details of an error returned by the Featureform gRPC server.
    """

    code: int
    message: str
    reason: str
    metadata: Dict[str, str] = field(default_factory=dict)

    @staticmethod
    def from_grpc_error(e: grpc.RpcError) -> Optional["FFGrpcErrorDetails"]:
        """
        from_grpc_error is a static method that creates a FFGrpcErrorDetails object from a gRPC error.
        """
        status_proto = _extract_error_details(e)

        for detail in status_proto.details:
            # should only be one detail
            if detail.Is(error_details_pb2.ErrorInfo.DESCRIPTOR):
                error_info = error_details_pb2.ErrorInfo()
                detail.Unpack(error_info)

                return FFGrpcErrorDetails(
                    code=status_proto.code,
                    message=status_proto.message,
                    reason=error_info.reason,
                    metadata=dict(error_info.metadata),
                )
            else:
                logging.debug("Unknown error detail type: %s", detail)
                return None


class GrpcClient:
    def __init__(self, grpc_stub, debug=False, insecure=False, host=None):
        self._grpc_stub = grpc_stub
        self._insecure = insecure
        self._host = host
        self.debug = debug
        self.expected_codes = [
            grpc.StatusCode.INTERNAL,
            grpc.StatusCode.NOT_FOUND,
            grpc.StatusCode.ALREADY_EXISTS,
            grpc.StatusCode.INVALID_ARGUMENT,
        ]

    def streaming_wrapper(self, multi_threaded_rendezvous):
        try:
            for message in multi_threaded_rendezvous:
                yield message
        except grpc.RpcError as e:
            # Handle the error gracefully here.
            self.handle_grpc_error(e)

    @staticmethod
    def is_streaming_response(obj):
        return hasattr(obj, "__iter__") and not isinstance(
            obj, (str, bytes, dict, list)
        )

    def __getattr__(self, name):
        attr = getattr(self._grpc_stub, name)

        def wrapper(*args, **kwargs):
            try:
                # Use the stored metadata for the call
                result = attr(*args, **kwargs)
                # If result is a streaming call, wrap it.
                if self.is_streaming_response(result):
                    return self.streaming_wrapper(result)
                return result
            except grpc.RpcError as e:
                self.handle_grpc_error(e)

        return wrapper

    def handle_grpc_error(self, e: grpc.RpcError) -> None:
        ex = e if self.debug else None

        with _limited_traceback(None if self.debug else 0):
            if e.code() in self.expected_codes:
                self._handle_expected_error(e)
            elif e.code() == grpc.StatusCode.UNAVAILABLE:
                raise Exception(
                    f"Could not connect to Featureform.\n"
                    "Please check if your FEATUREFORM_HOST and FEATUREFORM_CERT environment variables are set "
                    "correctly or are explicitly set in the client or command line.\n"
                    f"Details: {e.details()}"
                ) from ex
            elif e.code() == grpc.StatusCode.UNKNOWN:
                raise Exception(f"Error: {e.details()}") from ex
            else:
                raise e

    def _handle_expected_error(self, e: Optional[grpc.RpcError]) -> None:
        if self.debug:
            logging.debug("Processing expected gRPC error with details", exc_info=True)

        # With the introduction of new server errors, this extracts the details from the grpc error
        grpc_error_details = FFGrpcErrorDetails.from_grpc_error(e)
        if grpc_error_details:
            detailed_message = (
                f"{grpc_error_details.reason}: {grpc_error_details.message}\n"
                f"{_format_metadata(grpc_error_details.metadata)}"
            )
            if self.debug:
                logging.debug(detailed_message)
            raise Exception(detailed_message) from (e if self.debug else None)
        raise e


@contextlib.contextmanager
def _limited_traceback(limit):
    original_limit = getattr(sys, "tracebacklimit", None)
    sys.tracebacklimit = limit
    try:
        yield
    finally:
        sys.tracebacklimit = original_limit


def _extract_error_details(e: grpc.RpcError) -> status_pb2.Status:
    # "grpc-status-details-bin" is a binary representation of the status details,
    # and we cannot assume it will always be first or even present in metadata list
    status = status_pb2.Status()
    for key, value in e.trailing_metadata():
        if key == "grpc-status-details-bin":
            status.MergeFromString(value)
    return status


def _format_metadata(metadata):
    return "\n".join([f"{k}: {v}" for k, v in metadata.items()])
