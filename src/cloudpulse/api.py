"""FastAPI router. All behaviour lives in service.py / auth.py (unit tested without the framework).

Run:  cloudpulse serve        (or: uvicorn cloudpulse.api:create_app --factory)
Docs: http://localhost:8000/docs
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Literal

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel, Field, model_validator

from . import __version__
from .alerts import AlertManager, AlertThresholds, AsyncNotifier, LogNotifier, SNSNotifier
from .auth import PERMISSIONS, AuthError, Authenticator  # noqa: F401  (PERMISSIONS re-exported for callers)
from .config import Settings
from .service import MAX_OBJECT_BYTES, CloudPulseService, ServiceError
from .storage import SQLiteStore

log = logging.getLogger("cloudpulse.api")


class TransferIn(BaseModel):
    transfer_id: str = Field(min_length=1, max_length=64)
    direction: Literal["upload", "download"]
    bucket: str = Field(min_length=1, max_length=255)
    key: str = Field(min_length=1, max_length=1024)
    file_size_bytes: int = Field(ge=0, le=MAX_OBJECT_BYTES)
    started_at: float = Field(allow_inf_nan=False)
    completed_at: float = Field(allow_inf_nan=False)
    status: Literal["success", "failed"]
    latency_ms: float | None = Field(default=None, ge=0, allow_inf_nan=False)
    retries: int = Field(default=0, ge=0, le=1000)
    error_code: str | None = Field(default=None, max_length=128)
    error_message: str | None = Field(default=None, max_length=1000)
    instance_id: str = Field(default="local", max_length=64)
    cpu_percent: float | None = Field(default=None, ge=0, le=100, allow_inf_nan=False)
    memory_percent: float | None = Field(default=None, ge=0, le=100, allow_inf_nan=False)
    network_mbps: float | None = Field(default=None, ge=0, allow_inf_nan=False)

    @model_validator(mode="after")
    def _times(self):
        if self.completed_at < self.started_at:
            raise ValueError("completed_at must be >= started_at")
        return self


class BatchIn(BaseModel):
    transfers: list[TransferIn] = Field(min_length=1, max_length=1000)


def create_app(settings: Settings | None = None, store: SQLiteStore | None = None) -> FastAPI:
    settings = settings or Settings.from_env()
    if not settings.auth_disabled and not settings.api_keys:
        raise RuntimeError("Set CLOUDPULSE_API_KEYS=<key>:<admin|viewer|agent>,... "
                           "(or CLOUDPULSE_AUTH_DISABLED=true for local development only).")
    store = store or SQLiteStore(settings.db_path)
    notifiers = [LogNotifier()]
    if settings.sns_topic_arn:
        notifiers.append(AsyncNotifier(SNSNotifier(settings.sns_topic_arn, settings.aws_region)))
    manager = AlertManager(
        store, AlertThresholds(settings.min_throughput_mbps, settings.max_latency_ms, settings.max_retries),
        notifiers, settings.alert_cooldown_s)
    svc = CloudPulseService(store, settings, manager)
    auth = Authenticator(settings.api_keys, settings.auth_disabled)
    svc.maybe_purge()

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        yield
        svc.close()

    app = FastAPI(title="CloudPulse API", version=__version__, lifespan=lifespan,
                  description="Performance and reliability analytics for EC2-S3 transfers.")

    @app.exception_handler(ServiceError)
    async def _service_error(_: Request, exc: ServiceError):
        return JSONResponse({"detail": exc.message}, status_code=exc.status)

    def require(permission: str | None):
        def dependency(request: Request, x_api_key: str | None = Header(default=None)) -> str:
            try:
                return auth.authorize(x_api_key, permission)
            except AuthError as exc:
                log.warning("auth failure %s from %s on %s", exc.status,
                            request.client.host if request.client else "?", request.url.path)
                raise HTTPException(exc.status, exc.message)
        return dependency

    Direction = Query(default=None, pattern="^(upload|download)$")

    # ---- public ----------------------------------------------------------
    @app.get("/health", tags=["system"])
    def health():
        return svc.health()

    @app.get("/api/v1/whoami", tags=["system"])
    def whoami(role: str = Depends(require(None))):
        return {"role": role, "permissions": sorted(PERMISSIONS[role])}

    # ---- ingestion -------------------------------------------------------
    @app.post("/api/v1/transfers", status_code=201, tags=["ingest"])
    def ingest_one(item: TransferIn, _: str = Depends(require("ingest"))):
        return svc.ingest([item.model_dump()])

    @app.post("/api/v1/transfers/batch", status_code=201, tags=["ingest"])
    def ingest_batch(batch: BatchIn, _: str = Depends(require("ingest"))):
        return svc.ingest([t.model_dump() for t in batch.transfers])

    # ---- analytics -------------------------------------------------------
    @app.get("/api/v1/transfers", tags=["analytics"])
    def list_transfers(range: str = "24h", direction: str | None = Direction,
                       status: str | None = Query(default=None, pattern="^(success|failed)$"),
                       limit: int = Query(100, ge=1, le=1000), offset: int = Query(0, ge=0),
                       _: str = Depends(require("read"))):
        return svc.transfers(range, direction, status, limit, offset)

    @app.get("/api/v1/metrics/summary", tags=["analytics"])
    def summary(range: str = "24h", direction: str | None = Direction, _: str = Depends(require("read"))):
        return svc.summary(range, direction)

    @app.get("/api/v1/metrics/timeseries", tags=["analytics"])
    def series(range: str = "24h", direction: str | None = Direction, bucket: int | None = Query(None, ge=10),
               _: str = Depends(require("read"))):
        return svc.timeseries(range, direction, bucket)

    @app.get("/api/v1/reliability", tags=["analytics"])
    def reliability(range: str = "24h", direction: str | None = Direction, _: str = Depends(require("read"))):
        return svc.reliability(range, direction)

    # ---- alerts ----------------------------------------------------------
    @app.get("/api/v1/alerts", tags=["alerts"])
    def alerts(range: str = "24h", open_only: bool = False, limit: int = Query(100, ge=1, le=1000),
               _: str = Depends(require("read"))):
        return svc.alerts(range, open_only, limit)

    @app.post("/api/v1/alerts/{alert_id}/ack", tags=["alerts"])
    def ack(alert_id: int, _: str = Depends(require("admin"))):
        return svc.ack(alert_id)

    # ---- reports ---------------------------------------------------------
    @app.get("/api/v1/reports/{kind}", tags=["reports"])
    def report(kind: Literal["daily", "weekly"], format: Literal["json", "markdown"] = "json",
               _: str = Depends(require("read"))):
        out = svc.report(kind, format)
        return PlainTextResponse(out, media_type="text/markdown") if format == "markdown" else out

    app.state.service, app.state.settings = svc, settings
    return app
