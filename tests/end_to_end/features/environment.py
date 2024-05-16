import os
import uuid

import featureform as ff


def before_scenario(context, scenario):
    if (
        "use_autovariants" in scenario.tags
        or "use_autovariants" in scenario.feature.tags
    ):
        # Set the environment variable for the duration of the scenario
        os.environ["FF_GET_EQUIVALENT_VARIANTS"] = "true"

    prefix = uuid.uuid4()
    context.unique_prefix = str(prefix)[:5]

    ff.set_variant_prefix(context.unique_prefix)


def after_scenario(context, scenario):
    if (
        "use_autovariants" in scenario.tags
        or "use_autovariants" in scenario.feature.tags
    ):
        if "FF_GET_EQUIVALENT_VARIANTS" in os.environ:
            del os.environ["FF_GET_EQUIVALENT_VARIANTS"]
