"""
Disputes Management and HITL Review API.
Provides endpoints to monitor dispute lifecycles, inspect evidence, download PDFs, and perform human overrides.
"""

import os
import sqlite3
import uuid
from datetime import UTC, datetime

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from src.core.exceptions import DisputeNotFoundError
from src.models.arbitration import ArbitrationAction, ArbitrationDecision
from src.models.schemas import (
    BuyerInfo,
    DisputedTransaction,
    DisputeReason,
    DisputeRecord,
    DisputeStage,
    DisputeStatsSummary,
    DisputeStatus,
    HITLOverrideRequest,
    Money,
    PayPalDisputeResource,
    PayPalWebhookEvent,
    normalize_dispute_reason,
)
from src.workflows.dispute_pipeline import DisputePipeline, get_dispute_pipeline

router = APIRouter(prefix="/disputes", tags=["Dispute Operations & HITL"])


class SimulateDisputeRequest(BaseModel):
    """Payload to trigger an autonomous dispute resolution simulation."""

    order_id: str | None = Field(None, description="e.g. 'ORD-ZAP-10294' (Marcus Vance)")
    customer_name: str | None = Field(None, description="e.g. 'Marcus Vance'")
    customer_email: str | None = Field(None, description="e.g. 'marcus.vance@gmail.com'")
    reason: str = Field("MERCHANDISE_OR_SERVICE_NOT_RECEIVED", description="Dispute reason code")
    stage: str = Field("CLAIM", description="Stage: 'INQUIRY' or 'CLAIM'")
    amount: float | None = Field(None, description="Disputed amount, defaults to order total")
    merchant_id: str = Field("default-merchant", description="Tenant merchant ID")


@router.get(
    "",
    response_model=list[DisputeRecord],
    summary="List All Disputes",
    description="Retrieve all processed and pending disputes with optional filtering by stage or pipeline status.",
)
async def list_disputes(
    stage: str | None = Query(None, description="Filter by stage: INQUIRY, CLAIM, etc."),
    status: str | None = Query(
        None, description="Filter by status: DEFENDED, CONCEDED, FLAGGED_HITL, etc."
    ),
    pipeline: DisputePipeline = Depends(get_dispute_pipeline),
) -> list[DisputeRecord]:
    return pipeline.list_records(stage=stage, status=status)


@router.get(
    "/hitl/queue",
    response_model=list[DisputeRecord],
    summary="Human-in-the-Loop Review Queue",
    description="Lists all disputes flagged for manual operational sign-off (order > $300 or borderline P(win)).",
)
async def get_hitl_queue(
    pipeline: DisputePipeline = Depends(get_dispute_pipeline),
) -> list[DisputeRecord]:
    return pipeline.get_hitl_queue()


@router.get(
    "/stats/summary",
    response_model=DisputeStatsSummary,
    summary="Dispute Analytics Summary",
    description="Calculates total defended, conceded, fees avoided, and average win probabilities.",
)
async def get_stats_summary(
    pipeline: DisputePipeline = Depends(get_dispute_pipeline),
) -> DisputeStatsSummary:
    return pipeline.get_stats_summary()


@router.get(
    "/{dispute_id}",
    response_model=DisputeRecord,
    summary="Get Dispute Dossier",
    description="Fetches full telemetry, arbitration decision, and audit trail for a dispute.",
)
async def get_dispute(
    dispute_id: str,
    pipeline: DisputePipeline = Depends(get_dispute_pipeline),
) -> DisputeRecord:
    record = pipeline.get_record(dispute_id)
    if not record:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dispute '{dispute_id}' not found in registry.",
        )
    return record


_DEMO_DOSSIER_SPECS: dict[str, dict[str, str | float]] = {
    "PP-D-99412": {
        "order_id": "ORD-ZAP-99412",
        "customer_name": "Elena Rostova",
        "customer_email": "elena.rostova@example.com",
        "amount": 349.00,
        "reason": DisputeReason.MERCHANDISE_OR_SERVICE_NOT_AS_DESCRIBED.value,
        "stage": DisputeStage.CLAIM.value,
        "specialist_agent": "SNADAgent",
        "win_probability": 0.92,
        "action": ArbitrationAction.PROVIDE_EVIDENCE.value,
        "summary": "Outbound calibrated scale weight matched catalog tare weight (0.0% variance). Hardware serial SN-SHURE-SM7B-4412 logged and verified.",
    },
    "PP-D-77194": {
        "order_id": "ORD-ZAP-77194",
        "customer_name": "Samantha Cole",
        "customer_email": "samantha.cole@outlook.com",
        "amount": 299.99,
        "reason": DisputeReason.UNAUTHORISED.value,
        "stage": DisputeStage.CLAIM.value,
        "specialist_agent": "FraudAgent",
        "win_probability": 0.98,
        "action": ArbitrationAction.PROVIDE_EVIDENCE.value,
        "summary": "Visa Compelling Evidence 3.0 (CE 3.0) liability shift qualified: prior undisputed orders matching IP, device fingerprint, and confirmed shipping address.",
    },
    "PP-D-55210": {
        "order_id": "ORD-ZAP-55210",
        "customer_name": "Sophia Chen",
        "customer_email": "sophia.chen@enterprise.io",
        "amount": 79.00,
        "reason": DisputeReason.CANCELED_RECURRING_BILLING.value,
        "stage": DisputeStage.INQUIRY.value,
        "specialist_agent": "BillingAgent",
        "win_probability": 0.95,
        "action": ArbitrationAction.SEND_MESSAGE.value,
        "summary": "Customer assented to recurring Terms of Service at checkout. Active server sessions and telemetry recorded post-renewal.",
    },
    "PP-D-44102": {
        "order_id": "ORD-ZAP-44102",
        "customer_name": "Liam Smith",
        "customer_email": "liam.smith@example.com",
        "amount": 35.00,
        "reason": DisputeReason.OTHER.value,
        "stage": DisputeStage.CLAIM.value,
        "specialist_agent": "BillingAgent",
        "win_probability": 0.15,
        "action": ArbitrationAction.ACCEPT_CLAIM.value,
        "summary": "Billing credit discrepancy verified from customer support logs. Early concession recommended to avoid penalty fees.",
    },
    "PP-D-66305": {
        "order_id": "ORD-ZAP-66305",
        "customer_name": "Jordan Lee",
        "customer_email": "jordan.lee@techcorp.io",
        "amount": 849.00,
        "reason": DisputeReason.MERCHANDISE_OR_SERVICE_NOT_RECEIVED.value,
        "stage": DisputeStage.CLAIM.value,
        "specialist_agent": "INRAgent",
        "win_probability": 0.88,
        "action": ArbitrationAction.FLAG_FOR_HITL.value,
        "summary": "High order value ($849.00 > $300.00 threshold). Delivery confirmed with recipient signature 'J. LEE' under PayPal §11.",
    },
    "PP-D-88201": {
        "order_id": "ORD-ZAP-10294",
        "customer_name": "Marcus Vance",
        "customer_email": "marcus.vance@gmail.com",
        "amount": 249.99,
        "reason": DisputeReason.MERCHANDISE_OR_SERVICE_NOT_RECEIVED.value,
        "stage": DisputeStage.INQUIRY.value,
        "specialist_agent": "INRAgent",
        "win_probability": 0.94,
        "action": ArbitrationAction.PROVIDE_EVIDENCE.value,
        "summary": "Carrier verified delivery at confirmed address with courier drop-off GPS (34.0983, -118.3267) and signed recipient proof 'M. VANCE'.",
    },
}


async def _compile_pdf_for_dispute(
    dispute_id: str,
    pipeline: DisputePipeline,
    *,
    order_id: str,
    customer_name: str,
    customer_email: str,
    amount: float,
    currency: str,
    reason: DisputeReason,
    stage: DisputeStage,
    specialist_agent: str,
    win_probability: float,
    action: ArbitrationAction,
    rebuttal_statement: str,
    file_path: str,
    existing_record: DisputeRecord | None = None,
) -> tuple[str, str]:
    """Compile or rebuild a ReportLab evidentiary PDF dossier and return `(path, sha256)`."""
    order = (
        existing_record.telemetry_order if existing_record else None
    ) or await pipeline.order_service.get_order_by_transaction_or_id(
        order_id=order_id,
        disputed_amount=amount,
        customer_name=customer_name,
        customer_email=customer_email,
    )
    carrier = (
        existing_record.telemetry_carrier if existing_record else None
    ) or await pipeline.carrier_service.get_tracking(
        order_id=order_id,
        customer_name=customer_name,
        customer_email=customer_email,
    )
    warehouse = (
        existing_record.telemetry_warehouse if existing_record else None
    ) or await pipeline.warehouse_service.get_fulfillment_telemetry(order_id=order_id)
    billing = (
        existing_record.telemetry_billing if existing_record else None
    ) or await pipeline.billing_service.get_billing_telemetry(order_id=order_id, order=order)

    dispute_res = PayPalDisputeResource(
        dispute_id=dispute_id,
        create_time=(
            existing_record.created_at.isoformat()
            if existing_record
            else datetime.now(UTC).isoformat()
        ),
        dispute_amount=Money(currency_code=currency, value=f"{amount:.2f}"),
        dispute_stage=stage,
        dispute_reason=reason,
    )
    compliance_result = pipeline.compliance_engine.audit_dispute(dispute_res, carrier, order)
    ce3_result = pipeline.visa_ce3_engine.evaluate_ce3_eligibility(
        order=order,
        current_ip=order.checkout_ip,
        current_device_fingerprint=order.device_fingerprint,
    )
    decision = ArbitrationDecision(
        dispute_id=dispute_id,
        win_probability=win_probability,
        expected_value=round(win_probability * amount - (1.0 - win_probability) * 15.0, 2),
        recommended_action=action,
        hitl_flagged=bool(existing_record.hitl_flagged)
        if existing_record
        else action == ArbitrationAction.FLAG_FOR_HITL,
        hitl_reasons=list(existing_record.hitl_reasons) if existing_record else [],
        executive_summary=rebuttal_statement,
        rebuttal_statement=rebuttal_statement,
        specialist_agent=specialist_agent,
        persona_mode=(existing_record.persona_mode if existing_record else None)
        or "ADVERSARIAL_LEGAL_DEFENSE",
        llm_tier_used=(existing_record.llm_tier_used if existing_record else None)
        or "TIER_3_DETERMINISTIC_HEURISTIC",
        reasoning_steps=list(existing_record.reasoning_steps) if existing_record else [],
        seller_rights_met=(
            bool(existing_record.seller_rights_met)
            if existing_record and existing_record.seller_rights_met is not None
            else compliance_result.is_eligible
        ),
    )

    os.makedirs(os.path.dirname(file_path) or ".", exist_ok=True)
    _, sha256 = pipeline.pdf_generator.generate_rebuttal_dossier_with_hash(
        dispute=dispute_res,
        decision=decision,
        carrier=carrier,
        warehouse=warehouse,
        order=order,
        compliance_result=compliance_result,
        ce3_result=ce3_result,
        billing_result=billing,
        file_path=file_path,
    )
    return file_path, sha256


@router.get(
    "/{dispute_id}/pdf",
    summary="Download Evidentiary Rebuttal PDF",
    description="Returns the compiled ReportLab PDF dossier submitted to PayPal.",
)
async def download_pdf_dossier(
    dispute_id: str,
    pipeline: DisputePipeline = Depends(get_dispute_pipeline),
) -> FileResponse:
    record = pipeline.get_record(dispute_id)
    if not record:
        demo_spec = _DEMO_DOSSIER_SPECS.get(dispute_id)
        if not demo_spec:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Dispute '{dispute_id}' not found.",
            )
        demo_path = os.path.join(
            pipeline.pdf_generator.output_dir, f"dispute_{dispute_id}_evidence.pdf"
        )
        demo_pdf_path, demo_sha256 = await _compile_pdf_for_dispute(
            dispute_id,
            pipeline,
            order_id=str(demo_spec["order_id"]),
            customer_name=str(demo_spec["customer_name"]),
            customer_email=str(demo_spec["customer_email"]),
            amount=float(demo_spec["amount"]),
            currency="USD",
            reason=DisputeReason(str(demo_spec["reason"])),
            stage=DisputeStage(str(demo_spec["stage"])),
            specialist_agent=str(demo_spec["specialist_agent"]),
            win_probability=float(demo_spec["win_probability"]),
            action=ArbitrationAction(str(demo_spec["action"])),
            rebuttal_statement=str(demo_spec["summary"]),
            file_path=demo_path,
        )
        return FileResponse(
            path=demo_pdf_path,
            media_type="application/pdf",
            filename=f"PayPal_Evidence_{dispute_id}.pdf",
            headers={"X-Dossier-SHA256": demo_sha256},
        )

    stored_path = record.pdf_dossier_path
    if not stored_path:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No PDF evidentiary dossier found for dispute '{dispute_id}'.",
        )

    pdf_path: str = stored_path
    sha256: str | None = record.pdf_dossier_sha256
    if not os.path.exists(pdf_path):
        order_ref = record.order_id or f"ORD-{dispute_id}"
        pdf_path, sha256 = await _compile_pdf_for_dispute(
            dispute_id,
            pipeline,
            order_id=order_ref,
            customer_name=(record.telemetry_order.customer_name if record.telemetry_order else None)
            or "Verified Customer",
            customer_email=record.customer_email or "customer@lexishield.io",
            amount=record.amount,
            currency=record.currency,
            reason=record.reason,
            stage=record.stage,
            specialist_agent=record.specialist_agent or "DisputeSupervisor",
            win_probability=record.win_probability if record.win_probability is not None else 0.85,
            action=ArbitrationAction(
                record.recommended_action or ArbitrationAction.PROVIDE_EVIDENCE.value
            ),
            rebuttal_statement=record.rebuttal_statement or "Evidentiary dossier compiled.",
            file_path=pdf_path,
            existing_record=record,
        )
        record.pdf_dossier_sha256 = sha256

    headers: dict[str, str] = {}
    if sha256:
        headers["X-Dossier-SHA256"] = sha256

    return FileResponse(
        path=pdf_path,
        media_type="application/pdf",
        filename=f"PayPal_Evidence_{dispute_id}.pdf",
        headers=headers,
    )


@router.post(
    "/{dispute_id}/hitl/override",
    response_model=DisputeRecord,
    summary="Submit HITL Human Override",
    description="Human operator confirms or overrides action (DEFEND, CONCEDE, SEND_MESSAGE, MAKE_OFFER).",
)
async def override_hitl_decision(
    dispute_id: str,
    override_req: HITLOverrideRequest,
    pipeline: DisputePipeline = Depends(get_dispute_pipeline),
) -> DisputeRecord:
    try:
        return await pipeline.override_hitl(dispute_id=dispute_id, override_req=override_req)
    except DisputeNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to apply HITL override: {exc!s}",
        ) from exc


class DisputeMessageRequest(BaseModel):
    """A message to post into a dispute thread."""

    message: str = Field(..., min_length=1, max_length=2000)


@router.post(
    "/{dispute_id}/messages",
    response_model=DisputeRecord,
    summary="Send Message to Buyer",
    description="Sends a merchant-written message to the buyer through PayPal and stores it on the dispute.",
)
async def send_message_to_buyer(
    dispute_id: str,
    req: DisputeMessageRequest,
    pipeline: DisputePipeline = Depends(get_dispute_pipeline),
) -> DisputeRecord:
    try:
        return await pipeline.send_seller_message(
            dispute_id=dispute_id, message=req.message.strip()
        )
    except DisputeNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc


class SimulateBuyerMessageRequest(BaseModel):
    """Optional fixed buyer text; when omitted the buyer message is generated."""

    message: str | None = Field(None, min_length=1, max_length=2000)


@router.post(
    "/{dispute_id}/simulate-buyer-message",
    response_model=DisputeRecord,
    summary="Simulate Buyer Reply",
    description=(
        "Demo helper for buyer and merchant communication. Without a `message`, the buyer's first message "
        "is a template for the dispute reason and later ones are written by the AI playing the buyer. "
        "The message runs through the same path a real PayPal CUSTOMER.DISPUTE.UPDATED webhook takes, "
        "so the merchant's AI reply follows automatically."
    ),
)
async def simulate_buyer_message(
    dispute_id: str,
    req: SimulateBuyerMessageRequest | None = None,
    pipeline: DisputePipeline = Depends(get_dispute_pipeline),
) -> DisputeRecord:
    text = req.message.strip() if req and req.message else None
    try:
        return await pipeline.simulate_buyer_turn(dispute_id=dispute_id, message=text)
    except DisputeNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc


@router.post(
    "/simulate",
    response_model=DisputeRecord,
    summary="Simulate Inbound PayPal Dispute",
    description="Trigger the live autonomous dispute resolution pipeline against any seeded customer order (e.g. Marcus Vance INR).",
)
async def simulate_dispute(
    req: SimulateDisputeRequest,
    pipeline: DisputePipeline = Depends(get_dispute_pipeline),
) -> DisputeRecord:
    """
    Simulates incoming PayPal dispute webhooks for seeded customer orders.
    Pulls carrier telemetry, warehouse QC weights, and serial numbers directly from SQLite,
    executes Seller Protection compliance checks, computes win probability,
    and generates the evidentiary ReportLab PDF rebuttal dossier.
    """
    # 1. Query SQLite for order details safely
    row = None
    try:
        conn = sqlite3.connect("lexishield.db", timeout=5.0)
        c = conn.cursor()
        if req.order_id:
            row = c.execute(
                "SELECT order_id, transaction_id, customer_name, customer_email, total_amount, currency FROM orders WHERE order_id = ? OR transaction_id = ?",
                (req.order_id, req.order_id),
            ).fetchone()
        if not row and (req.customer_name or req.customer_email):
            row = c.execute(
                "SELECT order_id, transaction_id, customer_name, customer_email, total_amount, currency FROM orders WHERE customer_name = ? OR customer_email = ?",
                (req.customer_name or "", req.customer_email or ""),
            ).fetchone()
        conn.close()
    except sqlite3.Error:
        row = None

    order_id = row[0] if row else (req.order_id or "ORD-ZAP-10294")
    txn_id = row[1] if row else f"TXN-{order_id}"
    buyer_name = row[2] if row else (req.customer_name or "Marcus Vance")
    buyer_email = row[3] if row else (req.customer_email or "marcus.vance@gmail.com")
    amount = req.amount or (float(row[4]) if row else 249.99)
    currency = row[5] if row else "USD"

    # Map reason and stage strings to canonical enums
    try:
        stage_enum = DisputeStage(req.stage.upper())
    except ValueError:
        stage_enum = DisputeStage.CLAIM if "CLAIM" in req.stage.upper() else DisputeStage.INQUIRY

    reason_enum = normalize_dispute_reason(req.reason)

    sim_id = f"PP-SIM-{uuid.uuid4().hex[:8].upper()}"
    webhook_event = PayPalWebhookEvent(
        id=f"WH-EVT-{uuid.uuid4().hex[:12].upper()}",
        event_version="1.0",
        create_time=datetime.now(UTC).isoformat(),
        resource_type="dispute",
        event_type="CUSTOMER.DISPUTE.CREATED",
        summary=f"Simulated PayPal Dispute for {order_id} ({buyer_name})",
        resource=PayPalDisputeResource(
            dispute_id=sim_id,
            create_time=datetime.now(UTC).isoformat(),
            disputed_transactions=[
                DisputedTransaction(
                    buyer_transaction_id=txn_id,
                    seller_transaction_id=txn_id,
                    gross_amount=Money(currency_code=currency, value=f"{amount:.2f}"),
                    invoice_number=order_id,
                    custom=order_id,
                    buyer=BuyerInfo(name=buyer_name, email=buyer_email),
                )
            ],
            dispute_amount=Money(currency_code=currency, value=f"{amount:.2f}"),
            dispute_stage=stage_enum,
            dispute_reason=reason_enum,
            status=DisputeStatus.WAITING_FOR_SELLER_RESPONSE,
        ),
    )

    record = await pipeline.process_webhook_event(
        event=webhook_event,
        correlation_id=f"CORR-SIM-{sim_id}",
        merchant_id=req.merchant_id,
    )
    return record
