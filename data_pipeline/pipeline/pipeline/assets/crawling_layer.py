from dagster import asset

@asset(
    name="test_layer_asset",
    description="A simple test asset to verify Dagster configuration",
    compute_kind="python",
    group_name="test"
)
def test_layer_asset():
    """A simple asset that returns a greeting message."""
    return "Hello, Dagster World!"