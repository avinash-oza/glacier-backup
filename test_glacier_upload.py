from unittest import TestCase, mock
from glacier_upload import GlacierUploader, GlacierResult

@mock.patch('glacier_upload.glacier_upload.boto3')
class GlacierUploaderTestCase(TestCase):
    def test_init(self, mock_boto):
        g = GlacierUploader()
        self.assertIsNotNone(g)

    def test__upload_single_part(self, mock_boto3):
        mock_boto3.client.return_value.upload_archive.return_value = {'checksum': 'SAMPLE_CHECKSUM',
                                                                       'location': 'loca390ffh',
                                                                       'archiveId': 'ardfoifei290'}
        g = GlacierUploader()
        res = g._upload_single_part('archive_desc', mock.Mock(), 'my_path', 'VAULT_TEST')
        self.assertEqual(res.vault_name, 'VAULT_TEST')
        self.assertTrue(res.successful)
        self.assertFalse(res.multipart)
        self.assertEqual(res.tree_hash, 'SAMPLE_CHECKSUM')
        self.assertEqual(res.archive_id, 'ardfoifei290')
        self.assertIsNone(res.upload_id)

    def test__upload_single_part_exception(self, mock_boto3):
        mock_boto3.client.return_value.upload_archive.side_effect = ValueError
        
        g = GlacierUploader()
        res = g._upload_single_part('archive_desc', mock.Mock(), 'my_path', 'VAULT_TEST')
        self.assertEqual(res.vault_name, 'VAULT_TEST')
        self.assertFalse(res.successful)
        self.assertFalse(res.multipart)
        self.assertIsNone(res.tree_hash)
        self.assertIsNone(res.archive_id)
        self.assertIsNone(res.upload_id)
