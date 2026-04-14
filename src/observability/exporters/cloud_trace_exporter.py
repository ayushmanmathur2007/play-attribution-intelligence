"""Cloud Trace exporter stub — for GCP deployment."""


class CloudTraceExporter:
    """Export traces to Google Cloud Trace via OpenTelemetry.

    Stub implementation for GCP portability. Install opentelemetry-exporter-gcp-trace
    and implement the actual export logic when deploying to GCP.
    """

    def __init__(self, project_id: str, service_name: str = "play-attribution"):
        self.project_id = project_id
        self.service_name = service_name

    def export_trace(self, trace: dict):
        raise NotImplementedError(
            "CloudTraceExporter is a stub. Install opentelemetry-exporter-gcp-trace "
            "and implement the OpenTelemetry span export."
        )
