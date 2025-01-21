import pytest
from unittest.mock import Mock
from typing import Tuple

from featureform import Client, OnlineProvider, ResourceType


class TestCreateDeleteRequest:
    def test_string_input_without_resource_type(self):
        """Should raise ValueError when string input has no resource type"""
        client = Client()
        with pytest.raises(
            ValueError, match="resource_type must be specified if source is a string"
        ):
            client._create_delete_request("test_source")

    def test_string_input_provider(self):
        """Should create request for provider without requiring variant"""
        client = Client()
        request = client._create_delete_request(
            "test_provider", resource_type=ResourceType.PROVIDER
        )

        assert request.resource_id.resource.name == "test_provider"
        assert request.resource_id.resource.variant == ""
        assert request.resource_id.resource_type == ResourceType.PROVIDER.to_proto()

    def test_string_input_non_provider_without_variant(self):
        """Should raise ValueError for non-provider string input without variant"""
        client = Client()
        with pytest.raises(
            ValueError, match="variant must be specified for non-provider resources"
        ):
            client._create_delete_request(
                "test_feature", resource_type=ResourceType.FEATURE_VARIANT
            )

    def test_string_input_with_variant(self):
        """Should create request for string input with variant"""
        client = Client()
        request = client._create_delete_request(
            "test_feature", variant="v1", resource_type=ResourceType.FEATURE_VARIANT
        )

        assert request.resource_id.resource.name == "test_feature"
        assert request.resource_id.resource.variant == "v1"
        assert (
            request.resource_id.resource_type == ResourceType.FEATURE_VARIANT.to_proto()
        )

    def test_provider_input(self):
        """Should create request for provider object"""
        client = Client()

        # Mock provider
        provider = Mock(spec=OnlineProvider)
        provider.name.return_value = "test_provider"

        request = client._create_delete_request(provider)

        assert request.resource_id.resource.name == "test_provider"
        assert request.resource_id.resource.variant == ""
        assert request.resource_id.resource_type == ResourceType.PROVIDER.to_proto()

    def test_resource_input(self):
        """Should create request for resource object"""
        client = Client()

        # Mock resource implementing DeletableResource
        class MockResource:
            def name_variant(self) -> Tuple[str, str]:
                return "test_feature", "v1"

            def get_resource_type(self) -> ResourceType:
                return ResourceType.FEATURE_VARIANT

        resource = MockResource()
        request = client._create_delete_request(resource)

        assert request.resource_id.resource.name == "test_feature"
        assert request.resource_id.resource.variant == "v1"
        assert (
            request.resource_id.resource_type == ResourceType.FEATURE_VARIANT.to_proto()
        )
