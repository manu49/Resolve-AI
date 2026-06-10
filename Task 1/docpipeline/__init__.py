"""A concurrent document-processing pipeline of pluggable "microservices".

See ``DESIGN.md`` for the rationale. Quick start:

    from docpipeline import Document, Stage, PipelineOrchestrator
    from docpipeline.processors.examples import TextExtractor, LanguageDetector

    pipe = PipelineOrchestrator([
        Stage(TextExtractor(), workers=4),
        Stage(LanguageDetector(), workers=2),
    ])
    result = pipe.run([Document(content="...")])   # throughput-optimized
    result = pipe.run_per_document([...])           # latency-optimized
"""
from .document import Document
from .metrics import RunMetrics, StageMetrics
from .orchestrator import FailedDocument, PipelineOrchestrator, PipelineResult
from .policies import ExecMode, FailureAction, RetryPolicy, Stage
from .processor import DocumentProcessor, ProcessorError

__all__ = [
    "Document",
    "DocumentProcessor",
    "ProcessorError",
    "Stage",
    "RetryPolicy",
    "FailureAction",
    "ExecMode",
    "PipelineOrchestrator",
    "PipelineResult",
    "FailedDocument",
    "RunMetrics",
    "StageMetrics",
]
