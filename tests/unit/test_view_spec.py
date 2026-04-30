"""Test suite for the optional `DatasetConfig.views` metadata used by the
hierarchical schema endpoint to label and order each subsystem's views.

When `views` is None the endpoint discovers views by convention (the main
view is the one named exactly like the subsystem; `_dim_*` are dimension
views; `_all` is hidden). When `views` is set, it overrides the convention
and provides per-view PT/EN labels.
"""

from datasus_etl.datasets.base import DatasetConfig, ViewSpec


def test_viewspec_defaults_are_sensible() -> None:
    spec = ViewSpec(name="sihsus_dim_diag", role="dim")
    assert spec.name == "sihsus_dim_diag"
    assert spec.role == "dim"
    assert spec.label_pt is None
    assert spec.label_en is None
    assert spec.description is None


def test_viewspec_accepts_full_metadata() -> None:
    spec = ViewSpec(
        name="sihsus_dim_diag",
        role="dim",
        label_pt="Diagnósticos",
        label_en="Diagnoses",
        description="Two-column lookup: code → label.",
    )
    assert spec.label_pt == "Diagnósticos"
    assert spec.label_en == "Diagnoses"
    assert spec.description == "Two-column lookup: code → label."


def test_dataset_config_views_default_is_none() -> None:
    """An unconfigured DatasetConfig uses the discovery convention."""
    from datasus_etl.datasets.base import DatasetRegistry

    cls = DatasetRegistry.get("sihsus")
    assert cls is not None
    assert cls.views is None  # default → fall back to convention
