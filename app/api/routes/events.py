from fastapi import APIRouter, HTTPException, status

from app.api.dependencies import ProducerDependency
from app.metrics import EVENTS_PUBLISH_ERRORS, EVENTS_RECEIVED
from app.models import Event, EventAccepted

router = APIRouter(tags=["events"])


@router.post("/events", response_model=EventAccepted, status_code=status.HTTP_202_ACCEPTED)
async def publish_event(
    event: Event, producer: ProducerDependency
) -> EventAccepted:
    try:
        await producer.publish(str(event.event_id), event.kafka_payload())
    except Exception as exc:
        EVENTS_PUBLISH_ERRORS.inc()
        raise HTTPException(status_code=503, detail="Event broker is unavailable") from exc
    EVENTS_RECEIVED.inc()
    return EventAccepted(event_id=event.event_id)
