from unittest import TestCase, mock
from glacier_upload.glacier_upload import GlacierUploader, GlacierResult

@mock.patch('glacier_upload.glacier_upload.boto3')
class GlacierUploaderTestCase(TestCase):
    def test_init(self, mock_boto):
        g = GlacierUploader()
        self.assertIsNotNone(g)

