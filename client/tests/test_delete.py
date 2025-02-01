import pytest
from unittest.mock import Mock
from typing import Tuple

from featureform import (
    Client,
    ColumnSourceRegistrar,
    FeatureColumnResource,
    LabelColumnResource,
    OfflineProvider,
    OnlineProvider,
    ResourceType,
    SubscriptableTransformation,
    TrainingSetVariant,
    entity,
)

import pytest
from unittest.mock import Mock
from featureform import Client, OnlineProvider, ResourceType, entity


class TestDelete:
    @pytest.fixture
    def client(self):
        """Fixture to provide a fresh client instance with a mocked stub."""
        client = Client(host="localhost:7878", insecure=True, dry_run=True)
        client._stub = Mock()
        return client

    @pytest.mark.parametrize("method", ["delete", "prune"])
    def test_string_input_without_resource_type(self, client, method):
        """Should raise ValueError when string input has no resource type."""
        with pytest.raises(
            ValueError, match="resource_type must be specified if source is a string"
        ):
            getattr(client, method)("test_source")

    @pytest.mark.parametrize("method", ["delete", "prune"])
    def test_string_input_provider(self, client, method):
        """Should create a valid request for a provider without requiring variant."""
        getattr(client, method)("test_provider", resource_type=ResourceType.PROVIDER)

        stub_method = (
            client._stub.MarkForDeletion
            if method == "delete"
            else client._stub.PruneResource
        )
        stub_method.assert_called_once()

        request = stub_method.call_args[0][0]
        assert request.resource_id.resource.name == "test_provider"
        assert request.resource_id.resource.variant == ""
        assert request.resource_id.resource_type == ResourceType.PROVIDER.to_proto()

    @pytest.mark.parametrize("method", ["delete", "prune"])
    def test_string_input_non_provider_without_variant(self, client, method):
        """Should raise ValueError for non-provider string input without variant."""
        with pytest.raises(
            ValueError, match="variant must be specified for non-provider resources"
        ):
            getattr(client, method)(
                "test_feature", resource_type=ResourceType.FEATURE_VARIANT
            )

    @pytest.mark.parametrize("method", ["delete", "prune"])
    def test_string_input_with_variant(self, client, method):
        """Should create a valid request for string input with a variant."""
        getattr(client, method)(
            "source_variant", variant="v1", resource_type=ResourceType.SOURCE_VARIANT
        )

        stub_method = (
            client._stub.MarkForDeletion
            if method == "delete"
            else client._stub.PruneResource
        )
        stub_method.assert_called_once()

        request = stub_method.call_args[0][0]
        assert request.resource_id.resource.name == "source_variant"
        assert request.resource_id.resource.variant == "v1"
        assert (
            request.resource_id.resource_type == ResourceType.SOURCE_VARIANT.to_proto()
        )

    @pytest.mark.parametrize("method", ["delete", "prune"])
    def test_provider_input(self, client, method):
        """Should create a valid request for a provider object."""
        # Mock provider object
        provider = Mock(spec=OnlineProvider)
        provider.name.return_value = "test_provider"

        getattr(client, method)(provider)

        stub_method = (
            client._stub.MarkForDeletion
            if method == "delete"
            else client._stub.PruneResource
        )
        stub_method.assert_called_once()

        request = stub_method.call_args[0][0]
        assert request.resource_id.resource.name == "test_provider"
        assert request.resource_id.resource.variant == ""
        assert request.resource_id.resource_type == ResourceType.PROVIDER.to_proto()

    @pytest.mark.parametrize("method", ["delete", "prune"])
    def test_non_deletable_resource(self, client, method):
        """Should raise ValueError for a resource that does not implement DeletableResource."""

        # Mock a resource not implementing DeletableResource
        @entity
        class User:
            pass

        with pytest.raises(ValueError, match="resource is not deletable"):
            getattr(client, method)(User)

    @pytest.mark.parametrize(
        "resource_type,variant,name",
        [
            (ResourceType.PROVIDER, None, "test_provider"),
            (ResourceType.SOURCE_VARIANT, "v1", "test_source"),
            (ResourceType.FEATURE_VARIANT, "v1", "test_feature"),
            (ResourceType.LABEL_VARIANT, "v2", "test_label"),
            (ResourceType.TRAININGSET_VARIANT, "v3", "test_trainingset"),
            (ResourceType.TRANSFORMATION, "v4", "test_transformation"),
        ],
    )
    @pytest.mark.parametrize("method", ["delete", "prune"])
    def test_valid_deletable_resources(
        self, client, resource_type, variant, name, method
    ):
        """Should create valid requests for all deletable resource types."""
        if variant:
            getattr(client, method)(name, variant=variant, resource_type=resource_type)
        else:
            getattr(client, method)(name, resource_type=resource_type)

        # Determine the correct stub method
        stub_method = (
            client._stub.MarkForDeletion
            if method == "delete"
            else client._stub.PruneResource
        )
        stub_method.assert_called_once()

        # Validate the request object
        request = stub_method.call_args[0][0]
        assert request.resource_id.resource.name == name
        assert request.resource_id.resource.variant == (variant or "")
        assert request.resource_id.resource_type == resource_type.to_proto()

    @pytest.mark.parametrize(
        "resource_type",
        [
            "USER",
            "ENTITY",
            "ONDEMAND_FEATURE",
            "SCHEDULE",
            "MODEL",
        ],
    )
    @pytest.mark.parametrize("method", ["delete", "prune"])
    def test_invalid_resource_type(self, client, resource_type, method):
        """Should raise ValueError for resource types that are not deletable."""
        with pytest.raises(ValueError, match="resource_type must be deletable"):
            getattr(client, method)(
                "test_source",
                variant="v1",
                resource_type=getattr(ResourceType, resource_type, None),
            )

    @pytest.mark.parametrize(
        "resource_obj,expected_resource_type",
        [
            (Mock(spec=FeatureColumnResource), ResourceType.FEATURE_VARIANT),
            (Mock(spec=SubscriptableTransformation), ResourceType.TRANSFORMATION),
            (Mock(spec=LabelColumnResource), ResourceType.LABEL_VARIANT),
            (Mock(spec=TrainingSetVariant), ResourceType.TRAININGSET_VARIANT),
            (Mock(spec=ColumnSourceRegistrar), ResourceType.SOURCE_VARIANT),
            (Mock(spec=OnlineProvider), ResourceType.PROVIDER),
            (Mock(spec=OfflineProvider), ResourceType.PROVIDER),
        ],
    )
    @pytest.mark.parametrize("method", ["delete", "prune"])
    def test_deletable_resource_objects(
        self, client, resource_obj, expected_resource_type, method
    ):
        """Should create valid requests for all deletable resource objects."""
        # Mock the name_variant and get_resource_type methods for the resource object
        if expected_resource_type == ResourceType.PROVIDER:
            resource_obj.name.return_value = "test_resource"
        else:
            resource_obj.name_variant.return_value = ("test_resource", "v1")
            resource_obj.get_resource_type.return_value = expected_resource_type

        # Call the delete or prune method
        getattr(client, method)(resource_obj)

        # Determine the correct stub method
        stub_method = (
            client._stub.MarkForDeletion
            if method == "delete"
            else client._stub.PruneResource
        )
        stub_method.assert_called_once()

        # Validate the request object
        request = stub_method.call_args[0][0]
        assert request.resource_id.resource.name == "test_resource"
        if expected_resource_type == ResourceType.PROVIDER:
            assert request.resource_id.resource.variant == ""
        else:
            assert request.resource_id.resource.variant == "v1"
        assert request.resource_id.resource_type == expected_resource_type.to_proto()
