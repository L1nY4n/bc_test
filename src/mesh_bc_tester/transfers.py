from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from PySide6.QtCore import QObject, QTimer, Signal

from .models import DeviceProfile, TransferJob, TransferKind, new_id
from .protocol import build_transfer_packet, transfer_total_packets


PublishCallback = Callable[[DeviceProfile, dict, str], bool]


@dataclass(slots=True)
class _ActiveTransfer:
    job: TransferJob
    device: DeviceProfile
    data: bytes
    version: int
    voice_name: str
    packet_interval_ms: int
    next_index: int = 0
    next_due: float = 0.0


class TransferManager(QObject):
    job_updated = Signal(object)
    job_finished = Signal(object)
    system_message = Signal(str)

    def __init__(self, publish_callback: PublishCallback) -> None:
        super().__init__()
        self._publish_callback = publish_callback
        self._active: dict[str, _ActiveTransfer] = {}
        self._timer = QTimer(self)
        self._timer.setInterval(20)
        self._timer.timeout.connect(self._tick)

    def active_jobs(self) -> list[TransferJob]:
        return [entry.job for entry in self._active.values()]

    def start_jobs(
        self,
        *,
        devices: list[DeviceProfile],
        kind: TransferKind,
        file_path: str,
        version: int,
        voice_name: str,
        packet_interval_ms: int,
    ) -> list[TransferJob]:
        data = Path(file_path).read_bytes()
        jobs: list[TransferJob] = []
        for device in devices:
            if any(active.device.id == device.id for active in self._active.values()):
                self.system_message.emit(f"Transfer skipped for '{device.name}' because another transfer is active.")
                continue
            job = TransferJob(
                id=new_id(),
                device_id=device.id,
                device_name=device.name,
                kind=kind,
                file_path=file_path,
                status="running",
                total_packets=transfer_total_packets(kind, data),
                total_bytes=len(data),
            )
            self._active[job.id] = _ActiveTransfer(
                job=job,
                device=device,
                data=data,
                version=version,
                voice_name=voice_name,
                packet_interval_ms=max(packet_interval_ms, 1),
                next_due=time.monotonic(),
            )
            jobs.append(job)
            self.job_updated.emit(job)
        if self._active and not self._timer.isActive():
            self._timer.start()
        return jobs

    def cancel_jobs(self, job_ids: list[str]) -> None:
        for job_id in job_ids:
            active = self._active.pop(job_id, None)
            if active is None:
                continue
            active.job.status = "cancelled"
            self.job_finished.emit(active.job)
        if not self._active:
            self._timer.stop()

    def _tick(self) -> None:
        now = time.monotonic()
        finished: list[str] = []
        for job_id, active in list(self._active.items()):
            if now < active.next_due:
                continue
            if active.next_index >= active.job.total_packets:
                active.job.status = "completed"
                self.job_finished.emit(active.job)
                finished.append(job_id)
                continue
            payload, packet_bytes = build_transfer_packet(
                active.job.kind,
                active.data,
                active.next_index,
                version=active.version,
                voice_name=active.voice_name,
            )
            if not self._publish_callback(active.device, payload, "transfer"):
                active.job.status = "error"
                active.job.last_error = "Publish failed"
                self.job_finished.emit(active.job)
                finished.append(job_id)
                continue
            active.next_index += 1
            active.job.sent_packets = active.next_index
            active.job.sent_bytes = min(active.job.total_bytes, active.job.sent_bytes + packet_bytes)
            active.next_due = now + (active.packet_interval_ms / 1000)
            self.job_updated.emit(active.job)
        for job_id in finished:
            self._active.pop(job_id, None)
        if not self._active:
            self._timer.stop()
