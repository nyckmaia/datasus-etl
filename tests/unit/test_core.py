"""Tests for core pipeline components."""

import pytest

from datasus_etl.config import PipelineConfig
from datasus_etl.core.context import PipelineContext
from datasus_etl.core.pipeline import Pipeline
from datasus_etl.core.stage import Stage
from datasus_etl.exceptions import PyInmetError


class TestPipelineContext:
    """Tests for PipelineContext."""

    def test_context_initialization(self):
        """Test context initializes empty."""
        context = PipelineContext()

        assert not context.has_errors
        assert context.errors == []
        assert context.completed_stages == []

    def test_context_set_get(self):
        """Test setting and getting values."""
        context = PipelineContext()

        context.set("key1", "value1")
        context.set("key2", 42)

        assert context.get("key1") == "value1"
        assert context.get("key2") == 42
        assert context.get("nonexistent", "default") == "default"

    def test_context_has(self):
        """Test checking if key exists."""
        context = PipelineContext()

        context.set("key1", "value1")

        assert context.has("key1")
        assert not context.has("nonexistent")

    def test_context_metadata(self):
        """Test metadata operations."""
        context = PipelineContext()

        context.set_metadata("count", 100)
        context.set_metadata("name", "test")

        assert context.get_metadata("count") == 100
        assert context.get_metadata("name") == "test"
        assert context.get_metadata("nonexistent", "default") == "default"

    def test_context_errors(self):
        """Test error management."""
        context = PipelineContext()

        assert not context.has_errors

        context.add_error("Error 1")
        context.add_error("Error 2")

        assert context.has_errors
        assert context.errors == ["Error 1", "Error 2"]

    def test_context_completed_stages(self):
        """Test stage completion tracking."""
        context = PipelineContext()

        context.mark_stage_completed("Stage 1")
        context.mark_stage_completed("Stage 2")

        assert context.completed_stages == ["Stage 1", "Stage 2"]

    def test_context_to_dict(self):
        """Test converting context to dictionary."""
        context = PipelineContext()

        context.set("key1", "value1")
        context.set_metadata("meta1", "metavalue")
        context.add_error("Error 1")
        context.mark_stage_completed("Stage 1")

        context_dict = context.to_dict()

        assert context_dict["data"] == {"key1": "value1"}
        assert context_dict["metadata"] == {"meta1": "metavalue"}
        assert context_dict["errors"] == ["Error 1"]
        assert context_dict["completed_stages"] == ["Stage 1"]


class MockStage(Stage):
    """Mock stage for testing."""

    def __init__(self, name: str, should_fail: bool = False):
        super().__init__(name)
        self.should_fail = should_fail
        self.executed = False

    def _execute(self, context: PipelineContext) -> PipelineContext:
        """Execute mock stage."""
        self.executed = True

        if self.should_fail:
            raise PyInmetError(f"Mock stage {self.name} failed")

        context.set(f"{self.name}_result", "success")
        return context


class TestStage:
    """Tests for Stage."""

    def test_stage_initialization(self):
        """Test stage initialization."""
        stage = MockStage("TestStage")

        assert stage.name == "TestStage"
        assert not stage.executed

    def test_stage_execute_success(self):
        """Test successful stage execution."""
        stage = MockStage("TestStage")
        context = PipelineContext()

        result = stage.execute(context)

        assert stage.executed
        assert result.get("TestStage_result") == "success"
        assert "TestStage" in result.completed_stages

    def test_stage_execute_failure(self):
        """Test stage execution failure."""
        stage = MockStage("TestStage", should_fail=True)
        context = PipelineContext()

        with pytest.raises(PyInmetError):
            stage.execute(context)

        assert stage.executed
        assert context.has_errors

    def test_stage_chaining(self):
        """Test stage chaining with set_next."""
        stage1 = MockStage("Stage1")
        stage2 = MockStage("Stage2")
        stage3 = MockStage("Stage3")

        stage1.set_next(stage2).set_next(stage3)

        context = PipelineContext()
        result = stage1.execute(context)

        assert stage1.executed
        assert stage2.executed
        assert stage3.executed
        assert result.completed_stages == ["Stage1", "Stage2", "Stage3"]


class MockPipeline(Pipeline[PipelineConfig]):
    """Mock pipeline for testing."""

    def __init__(self, config: PipelineConfig, fail_stage: bool = False):
        super().__init__(config)
        self.fail_stage = fail_stage

    def setup_stages(self) -> None:
        """Set up mock stages."""
        self.add_stage(MockStage("Stage1"))
        self.add_stage(MockStage("Stage2", should_fail=self.fail_stage))
        self.add_stage(MockStage("Stage3"))


class TestPipeline:
    """Tests for Pipeline."""

    def test_pipeline_initialization(self, temp_dir):
        """Test pipeline initialization."""
        from datasus_etl.config import (
            ConversionConfig,
            DownloadConfig,
            ProcessingConfig,
            StorageConfig,
        )

        config = PipelineConfig(
            download=DownloadConfig(output_dir=temp_dir),
            conversion=ConversionConfig(
                dbc_dir=temp_dir,
                dbf_dir=temp_dir,
                csv_dir=temp_dir,
                tabwin_dir=temp_dir,
            ),
            processing=ProcessingConfig(input_dir=temp_dir, output_dir=temp_dir),
            storage=StorageConfig(parquet_dir=temp_dir),
        )

        pipeline = MockPipeline(config)

        assert pipeline.config == config
        assert isinstance(pipeline.context, PipelineContext)
        assert pipeline.stages == []

    def test_pipeline_add_stage(self, temp_dir):
        """Test adding stages to pipeline."""
        from datasus_etl.config import (
            ConversionConfig,
            DownloadConfig,
            ProcessingConfig,
            StorageConfig,
        )

        config = PipelineConfig(
            download=DownloadConfig(output_dir=temp_dir),
            conversion=ConversionConfig(
                dbc_dir=temp_dir,
                dbf_dir=temp_dir,
                csv_dir=temp_dir,
                tabwin_dir=temp_dir,
            ),
            processing=ProcessingConfig(input_dir=temp_dir, output_dir=temp_dir),
            storage=StorageConfig(parquet_dir=temp_dir),
        )

        pipeline = MockPipeline(config)
        stage = MockStage("TestStage")

        pipeline.add_stage(stage)

        assert len(pipeline.stages) == 1
        assert pipeline.stages[0] == stage

    def test_pipeline_run_success(self, temp_dir):
        """Test successful pipeline execution."""
        from datasus_etl.config import (
            ConversionConfig,
            DownloadConfig,
            ProcessingConfig,
            StorageConfig,
        )

        config = PipelineConfig(
            download=DownloadConfig(output_dir=temp_dir),
            conversion=ConversionConfig(
                dbc_dir=temp_dir,
                dbf_dir=temp_dir,
                csv_dir=temp_dir,
                tabwin_dir=temp_dir,
            ),
            processing=ProcessingConfig(input_dir=temp_dir, output_dir=temp_dir),
            storage=StorageConfig(parquet_dir=temp_dir),
        )

        pipeline = MockPipeline(config, fail_stage=False)
        result = pipeline.run()

        assert not result.has_errors
        assert len(result.completed_stages) == 3
        assert result.get("Stage1_result") == "success"
        assert result.get("Stage2_result") == "success"
        assert result.get("Stage3_result") == "success"

    def test_pipeline_run_failure(self, temp_dir):
        """Test pipeline execution with failure."""
        from datasus_etl.config import (
            ConversionConfig,
            DownloadConfig,
            ProcessingConfig,
            StorageConfig,
        )

        config = PipelineConfig(
            download=DownloadConfig(output_dir=temp_dir),
            conversion=ConversionConfig(
                dbc_dir=temp_dir,
                dbf_dir=temp_dir,
                csv_dir=temp_dir,
                tabwin_dir=temp_dir,
            ),
            processing=ProcessingConfig(input_dir=temp_dir, output_dir=temp_dir),
            storage=StorageConfig(parquet_dir=temp_dir),
        )

        pipeline = MockPipeline(config, fail_stage=True)

        with pytest.raises(PyInmetError):
            pipeline.run()
