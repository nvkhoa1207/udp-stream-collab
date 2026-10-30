import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from client.files import FileReceiver, build_metadata, iter_chunks


class FileReceiverTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.now = 0.0
        self.receiver = FileReceiver(self.root / 'received', clock=lambda: self.now)
        self.source = self.root / 'dữ liệu.bin'
        self.source.write_bytes(b'A' * 480 + b'B')
        self.meta = build_metadata(self.source)
        self.id = 'a' * 32

    def begin(self, metadata=None):
        self.receiver.begin(self.id, self.meta if metadata is None else metadata)

    def assert_clean(self):
        self.assertEqual(self.receiver.reserved_bytes, 0)
        self.assertFalse(self.receiver.transfers)
        self.assertEqual(list(self.receiver.output_directory.glob('*.part')), [])

    def test_reorder_duplicate_and_success(self):
        self.begin()
        for index in (1, 0, 0):
            self.receiver.accept_chunk(self.id, index, list(iter_chunks(self.source))[index])
        saved = self.receiver.finish(self.id)
        self.assertEqual(saved.read_bytes(), self.source.read_bytes())
        self.assert_clean()

    def test_empty_and_uppercase_hash(self):
        self.source.write_bytes(b'')
        metadata = build_metadata(self.source)
        metadata['sha256'] = metadata['sha256'].upper()
        self.begin(metadata)
        self.assertEqual(self.receiver.finish(self.id).read_bytes(), b'')
        self.assert_clean()

    def test_metadata_rejected_before_allocation(self):
        cases = [None, [], {}, dict(self.meta, extra=1)]
        for key, values in {
            'name': [1, '', '../x', '/x', 'x\\y', 'a\0b', 'CON.txt', 'a:', 'x.', 'ệ' * 43, '\ud800'],
            'size': [True, -1, '481', 17 * 1024 * 1024],
            'chunk_size': [True, 0, 481, 1.0],
            'total_chunks': [True, -1, 1, '2'],
            'sha256': [None, 'a'*63, 'g'*64, '0'*63+'\n'],
        }.items():
            cases.extend(dict(self.meta, **{key: value}) for value in values)
        cases.append(dict(self.meta, size=0, total_chunks=0, sha256='0'*64))
        for metadata in cases:
            with self.subTest(metadata=metadata), self.assertRaises(ValueError):
                self.receiver.begin(self.id, metadata)
            self.assert_clean()

    def test_transfer_id_and_existing_temp(self):
        for transfer_id in [None, '../bad', 'x'*32, 'a'*100, True]:
            with self.assertRaises(ValueError):
                self.receiver.begin(transfer_id, self.meta)
        path = self.receiver.output_directory / (self.id+'.part')
        path.write_bytes(b'existing')
        with self.assertRaises(FileExistsError):
            self.begin()
        self.assertEqual(path.read_bytes(), b'existing')
        self.assertFalse(self.receiver.transfers)

    def test_quota_reused_after_cancel(self):
        self.receiver = FileReceiver(self.root/'quota', max_file_bytes=500,
                                     max_total_bytes=481, max_transfers=1)
        self.begin()
        with self.assertRaises(ValueError):
            self.receiver.begin('b'*32, self.meta)
        self.assertEqual(self.receiver.reserved_bytes, 481)
        self.assertTrue(self.receiver.cancel(self.id))
        self.assertFalse(self.receiver.cancel(self.id))
        self.begin()
        self.receiver.close()
        self.assert_clean()

    def test_total_byte_and_chunk_quotas(self):
        self.receiver = FileReceiver(self.root/'quota', max_total_bytes=600, max_transfers=4)
        self.begin()
        with self.assertRaises(ValueError):
            self.receiver.begin('b'*32, self.meta)
        self.receiver.close()
        self.receiver = FileReceiver(self.root/'chunks', max_chunks=1)
        with self.assertRaises(ValueError):
            self.begin()
        self.assert_clean()

    def test_timeout_duplicate_does_not_refresh(self):
        self.begin()
        self.receiver.accept_chunk(self.id, 0, b'A'*480)
        self.now = 44
        self.receiver.accept_chunk(self.id, 0, b'A'*480)
        self.now = 45
        self.assertEqual(self.receiver.expire(), [self.id])
        self.assert_clean()
        self.begin()
        self.now = 90
        with self.assertRaises(TimeoutError):
            self.receiver.accept_chunk(self.id, 1, b'B')
        self.assert_clean()

    def test_bad_chunk_cleans_transfer(self):
        for index, data in [(True,b'A'*480), (2,b'B'), (0,b'bad'), (0,'A'*480)]:
            self.begin()
            with self.assertRaises(ValueError):
                self.receiver.accept_chunk(self.id, index, data)
            self.assert_clean()

    def test_missing_chunk_and_bad_hash_cleanup(self):
        self.begin()
        with self.assertRaises(ValueError):
            self.receiver.finish(self.id)
        self.assert_clean()
        self.begin(dict(self.meta, sha256='0'*64))
        self.receiver.accept_chunk(self.id, 0, b'A'*480)
        self.receiver.accept_chunk(self.id, 1, b'B')
        with self.assertRaises(ValueError):
            self.receiver.finish(self.id)
        self.assert_clean()
        self.assertFalse(list(self.receiver.output_directory.glob('*_*.bin')))

    def test_write_failure_cleanup(self):
        self.begin()
        with patch.object(Path, 'open', side_effect=OSError('disk full')):
            with self.assertRaises(OSError):
                self.receiver.accept_chunk(self.id, 0, b'A'*480)
        self.assert_clean()

    def test_create_failure_has_no_reserved_quota(self):
        with patch.object(Path, 'open', side_effect=PermissionError('read only')):
            with self.assertRaises(PermissionError):
                self.begin()
        self.assert_clean()

    def test_close_failure_after_create_cleans_owned_file(self):
        original = Path.open

        class CloseFailure:
            def __init__(self, handle):
                self.handle = handle

            def __enter__(self):
                return self.handle

            def __exit__(self, *args):
                self.handle.close()
                raise OSError('close failed')

        def open_file(path, *args, **kwargs):
            return CloseFailure(original(path, *args, **kwargs))

        with patch.object(Path, 'open', open_file):
            with self.assertRaises(OSError):
                self.begin()
        self.assert_clean()

    def test_cleanup_failure_retains_quota_and_retries(self):
        self.begin()
        with patch.object(Path, 'unlink', side_effect=PermissionError('busy')):
            with self.assertRaises(PermissionError):
                self.receiver.cancel(self.id)
        self.assertEqual(self.receiver.reserved_bytes, 481)
        self.assertTrue(self.receiver.transfers[self.id]['failed'])
        self.assertEqual(self.receiver.expire(), [self.id])
        self.assert_clean()

    def test_close_attempts_other_transfers_when_one_unlink_fails(self):
        self.begin()
        self.receiver.begin('b'*32, self.meta)
        original = Path.unlink
        def unlink(path, *args, **kwargs):
            if path.name == self.id+'.part':
                raise PermissionError('busy')
            return original(path, *args, **kwargs)
        with patch.object(Path, 'unlink', unlink):
            with self.assertRaises(ExceptionGroup):
                self.receiver.close()
        self.assertEqual(list(self.receiver.transfers), [self.id])
        self.receiver.close()
        self.assert_clean()

    def test_destination_created_after_begin_is_preserved(self):
        self.begin()
        target = self.receiver.transfers[self.id]['final_path']
        target.write_bytes(b'existing')
        for i, chunk in enumerate(iter_chunks(self.source)):
            self.receiver.accept_chunk(self.id, i, chunk)
        with self.assertRaises(FileExistsError):
            self.receiver.finish(self.id)
        self.assertEqual(target.read_bytes(), b'existing')
        self.assert_clean()

    def test_publish_cleanup_retry_preserves_complete_file(self):
        self.begin()
        for i, chunk in enumerate(iter_chunks(self.source)):
            self.receiver.accept_chunk(self.id, i, chunk)
        with patch.object(Path, 'unlink', side_effect=PermissionError('busy')):
            with self.assertRaises(PermissionError):
                self.receiver.finish(self.id)
        self.assertEqual(self.receiver.reserved_bytes, 481)
        target = self.receiver.finish(self.id)
        self.assertEqual(target.read_bytes(), self.source.read_bytes())
        self.assert_clean()


if __name__ == '__main__':
    unittest.main()
