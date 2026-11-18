"""Turn uploads and new versions of watched files into immutable inputs."""

import hashlib
import logging
import os
import sqlite3
import stat
import tempfile
import threading
from contextlib import closing
from pathlib import Path


MAX_FILE_BYTES = 15 * 1024 * 1024
MAX_INITIAL_BYTES = 40 * 1024 * 1024
log = logging.getLogger(__name__)


class Inbox:
    def __init__(self, root, state_dir, submit, *, subscriptions, interval=2):
        self.root = Path(root).expanduser().resolve()
        state = Path(state_dir).expanduser().resolve()
        if state == self.root or self.root in state.parents or state in self.root.parents:
            raise ValueError("投递目录和服务数据目录需要各自独立")
        self.root.mkdir(parents=True, exist_ok=True)
        self.snapshots = state / "materials"
        self.snapshots.mkdir(parents=True, exist_ok=True)
        self.database = state / "inbox.sqlite3"
        self.submit = submit
        self.subscriptions = subscriptions
        self.interval = interval
        self._stop = threading.Event()
        self._thread = None
        self._versions = {}
        with closing(sqlite3.connect(self.database)) as db, db:
            db.execute("CREATE TABLE IF NOT EXISTS revisions (id TEXT PRIMARY KEY)")

    def folders(self):
        return [str(folder.relative_to(self.root)) for folder, _ in self._walk(self.root)
                if folder != self.root]

    def prepare_watch(self, relative):
        path = Path(relative)
        if path.is_absolute() or any(part.startswith(".") for part in path.parts):
            raise ValueError("请选择投递目录中的子目录")
        folder = self.root / path
        if folder == self.root or folder != folder.resolve() or not folder.is_dir():
            raise ValueError("这个投递子目录不存在或不可订阅")
        return str(folder)

    def store_upload(self, filename, data):
        if (not filename or filename in (".", "..") or Path(filename).name != filename
                or "\\" in filename or any(ord(char) < 32 for char in filename)):
            raise ValueError("附件文件名无效")
        if len(data) > MAX_FILE_BYTES:
            raise ValueError(f"单个附件最多 {MAX_FILE_BYTES // (1024 * 1024)} MB")
        digest = hashlib.sha256(data).hexdigest()
        directory = self.snapshots / digest
        directory.mkdir(exist_ok=True)
        target = directory / filename
        if target.exists():
            return str(target)
        with tempfile.NamedTemporaryFile(dir=directory, delete=False) as output:
            temporary = Path(output.name)
            try:
                output.write(data)
                output.close()
                os.replace(temporary, target)
            finally:
                temporary.unlink(missing_ok=True)
        return str(target)

    @staticmethod
    def _walk(root):
        for directory, names, files in os.walk(root):
            folder = Path(directory)
            names[:] = sorted(name for name in names if not name.startswith(".")
                              and not (folder / name).is_symlink())
            yield folder, [folder / name for name in sorted(files) if not name.startswith(".")]

    def _material(self, folder, path):
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode):
            return None
        if info.st_size > MAX_FILE_BYTES:
            raise ValueError(f"文件太大：{path.name}")
        flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
        with os.fdopen(os.open(path, flags), "rb") as file:
            before = os.fstat(file.fileno())
            if not stat.S_ISREG(before.st_mode):
                return None
            data = file.read(MAX_FILE_BYTES + 1)
            after = os.fstat(file.fileno())
        version = self._version(info)
        if self._version(before) != version or self._version(after) != version:
            raise OSError(f"文件仍在写入：{path.name}")
        snapshot = self.store_upload(path.name, data)
        identity = f"{folder}\0{path}\0{before.st_mtime_ns}\0{Path(snapshot).parent.name}"
        return hashlib.sha256(identity.encode("utf-8")).hexdigest(), snapshot, version

    @staticmethod
    def _version(info):
        return info.st_dev, info.st_ino, info.st_mtime_ns, info.st_size

    def include_existing(self, watch):
        folder = Path(watch)
        self.prepare_watch(folder.relative_to(self.root))
        already_watched = watch in set(self.subscriptions())
        materials = []
        size = 0
        for _, files in self._walk(folder):
            for path in files:
                material = self._material(folder, path)
                if material:
                    size += Path(material[1]).stat().st_size
                    if size > MAX_INITIAL_BYTES or len(materials) >= 100:
                        raise ValueError("订阅目录的初始资料最多 100 个文件、合计 40 MB")
                    materials.append(material)
        if not already_watched:
            with closing(sqlite3.connect(self.database, timeout=10)) as db, db:
                db.executemany("INSERT OR IGNORE INTO revisions VALUES (?)",
                               [(revision,) for revision, _, _ in materials])
        return [snapshot for _, snapshot, _ in materials]

    def scan(self):
        seen = set()
        for watch in dict.fromkeys(self.subscriptions()):
            folder = Path(watch)
            try:
                self.prepare_watch(folder.relative_to(self.root))
            except (ValueError, OSError):
                continue
            with closing(sqlite3.connect(self.database, timeout=10)) as db:
                for _, files in self._walk(folder):
                    for path in files:
                        seen.add((folder, path))
                        self._publish(db, folder, path)
        self._versions = {key: version for key, version in self._versions.items() if key in seen}

    def _publish(self, db, folder, path):
        try:
            key = folder, path
            if self._versions.get(key) == self._version(path.lstat()):
                return
            material = self._material(folder, path)
        except (ValueError, OSError) as error:
            log.debug("Waiting for a stable inbox file %s: %s", path, error)
            return
        if not material:
            return
        revision, snapshot, version = material
        if db.execute("SELECT 1 FROM revisions WHERE id = ?", (revision,)).fetchone():
            self._versions[key] = version
            return
        name = path.relative_to(folder)
        self.submit(f"订阅目录收到文件：{name}", files=[snapshot], source="directory",
                    thread=str(folder), source_id=revision)
        with db:
            db.execute("INSERT OR IGNORE INTO revisions VALUES (?)", (revision,))
        self._versions[key] = version

    def _poll(self):
        while not self._stop.is_set():
            try:
                self.scan()
            except Exception:
                log.exception("Inbox scan failed and will be retried")
            self._stop.wait(self.interval)

    def start(self):
        self._thread = threading.Thread(target=self._poll, name="inbox", daemon=True)
        self._thread.start()

    def close(self):
        self._stop.set()
        if self._thread:
            self._thread.join()
