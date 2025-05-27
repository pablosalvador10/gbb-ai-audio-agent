from fastapi import APIRouter
from . import realtime, acs, health

router = APIRouter()
for r in (realtime.router, acs.router, health.router):
    router.include_router(r)
