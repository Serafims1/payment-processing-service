import json
import logging
import secrets
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated, cast
from uuid import UUID

from fastapi import Depends, FastAPI, Header, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
from sqlalchemy import text

from payments.config import Settings
from payments.database import Database
from payments.exceptions import ApplicationError, IdempotencyConflict, PaymentNotFound
from payments.models import Payment
from payments.repositories import PaymentRepository
from payments.schemas import PaymentAccepted, PaymentCreate, PaymentDetail
from payments.service import PaymentService

logger = logging.getLogger(__name__)


def detail(payment: Payment) -> PaymentDetail:
    return PaymentDetail.model_validate(
        {
            "payment_id": payment.id,
            "status": payment.status,
            "created_at": payment.created_at,
            "amount": payment.amount,
            "currency": payment.currency,
            "description": payment.description,
            "metadata": payment.metadata_json,
            "webhook_url": payment.webhook_url,
            "processed_at": payment.processed_at,
        }
    )


def service(request: Request) -> PaymentService:
    return cast(PaymentService, request.app.state.service)


def create_app(settings: Settings | None = None) -> FastAPI:
    config = settings or Settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        db = Database(config)
        app.state.database = db
        app.state.service = PaymentService(PaymentRepository(db.sessions))
        try:
            yield
        finally:
            await db.close()

    app = FastAPI(title="Payment processing", lifespan=lifespan)

    @app.exception_handler(RequestValidationError)
    async def validation_error(request: Request, exc: RequestValidationError) -> Response:
        # Invalid input can contain NaN or lone surrogates; don't echo it into a JSON response.
        errors = [{key: error[key] for key in ("loc", "msg", "type")} for error in exc.errors()]
        return Response(
            content=json.dumps({"detail": errors}, ensure_ascii=True),
            status_code=422,
            media_type="application/json",
        )

    async def authenticate(x_api_key: Annotated[str | None, Header()] = None) -> None:
        if x_api_key is None or not secrets.compare_digest(
            x_api_key.encode(), config.api_key.get_secret_value().encode()
        ):
            from fastapi import HTTPException

            raise HTTPException(status_code=401, detail="Invalid API key")

    @app.exception_handler(ApplicationError)
    async def application_error(request: Request, exc: ApplicationError) -> JSONResponse:
        code = 404 if isinstance(exc, PaymentNotFound) else 409
        if not isinstance(exc, (PaymentNotFound, IdempotencyConflict)):
            code = 500
        if code == 500:
            logger.error("Unexpected application error", exc_info=exc)
        message = str(exc) if code != 500 else "Internal server error"
        return JSONResponse(status_code=code, content={"detail": message})

    @app.exception_handler(Exception)
    async def unexpected_error(request: Request, exc: Exception) -> JSONResponse:
        logger.error("Unexpected API error", exc_info=exc)
        return JSONResponse(status_code=500, content={"detail": "Internal server error"})

    @app.get("/health")
    async def health(request: Request) -> dict[str, str]:
        db: Database = request.app.state.database
        async with db.sessions() as session:
            await session.execute(text("SELECT 1"))
        return {"status": "ok"}

    @app.post(
        "/api/v1/payments",
        response_model=PaymentAccepted,
        status_code=status.HTTP_202_ACCEPTED,
        dependencies=[Depends(authenticate)],
    )
    async def create_payment(
        body: PaymentCreate,
        idempotency_key: Annotated[str, Header(min_length=1, max_length=200)],
        application: Annotated[PaymentService, Depends(service)],
    ) -> PaymentAccepted:
        payment = await application.create(idempotency_key, body)
        return PaymentAccepted.model_validate(
            {"payment_id": payment.id, "status": payment.status, "created_at": payment.created_at}
        )

    @app.get(
        "/api/v1/payments/{payment_id}",
        response_model=PaymentDetail,
        dependencies=[Depends(authenticate)],
    )
    async def get_payment(
        payment_id: UUID,
        application: Annotated[PaymentService, Depends(service)],
    ) -> PaymentDetail:
        return detail(await application.get(payment_id))

    return app
