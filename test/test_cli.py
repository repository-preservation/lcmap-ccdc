from ccdc import cli
from ccdc import cli
from click.testing import CliRunner

import test

@test.vcr.use_cassette(test.cassette)
def test_changedetection():
    runner = CliRunner()
    result = runner.invoke(cli.entrypoint, ['changedetection', '-x 0', '-y 0', '-n 1'])
    print(result.output)
    assert result.exit_code == 0


def test_classification():
    pass
