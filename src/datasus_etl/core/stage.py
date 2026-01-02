"""Base Stage class for pipeline steps."""

import logging
from abc import ABC, abstractmethod
from typing import Optional

from datasus_etl.core.context import PipelineContext
from datasus_etl.exceptions import PipelineCancelled, PyInmetError


class Stage(ABC):
    """Abstract base class for pipeline stages.

    Each stage represents a step in the data processing pipeline.
    Stages are executed sequentially and can pass data through context.
    """

    def __init__(self, name: str) -> None:
        """Initialize the stage.

        Args:
            name: Name of the stage for logging and identification
        """
        self.name = name
        self.logger = logging.getLogger(f"datasus_etl.{name}")
        self._next_stage: Optional[Stage] = None

    @abstractmethod
    def _execute(self, context: PipelineContext) -> PipelineContext:
        """Execute the stage logic.

        This method must be implemented by subclasses.

        Args:
            context: Pipeline context with shared state

        Returns:
            Updated context after stage execution

        Raises:
            PyInmetError: If stage execution fails
        """
        pass

    def execute(self, context: PipelineContext) -> PipelineContext:
        """Execute the stage with error handling.

        Args:
            context: Pipeline context

        Returns:
            Updated context

        Raises:
            PyInmetError: If stage execution fails critically
        """
        # Get stage number for logging
        current_stage = context.get_metadata("current_stage", 1)
        total_stages = context.get_metadata("total_stages", 1)

        self.logger.debug(f"[{current_stage}/{total_stages}] Starting: {self.name}")

        try:
            context = self._execute(context)
            context.mark_stage_completed(self.name)
            self.logger.debug(f"[{current_stage}/{total_stages}] Completed: {self.name}")

        except PipelineCancelled:
            self.logger.info(f"[{current_stage}/{total_stages}] Cancelled: {self.name}")
            raise

        except PyInmetError as e:
            self.logger.error(f"[{current_stage}/{total_stages}] Failed: {self.name} - {e}")
            context.add_error(f"{self.name}: {e}")
            raise

        except Exception as e:
            self.logger.exception(f"[{current_stage}/{total_stages}] Error in: {self.name}")
            context.add_error(f"{self.name}: Unexpected error - {e}")
            raise PyInmetError(f"Stage {self.name} failed: {e}") from e

        # Chain to next stage if exists
        if self._next_stage:
            return self._next_stage.execute(context)

        return context

    def set_next(self, stage: "Stage") -> "Stage":
        """Set the next stage in the chain.

        Implements Chain of Responsibility pattern.

        Args:
            stage: The next stage to execute

        Returns:
            The next stage (for chaining)
        """
        self._next_stage = stage
        return stage

    def __repr__(self) -> str:
        """String representation of the stage."""
        return f"{self.__class__.__name__}(name={self.name!r})"
