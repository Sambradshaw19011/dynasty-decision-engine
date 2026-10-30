from src.main import main


def test_main_runs(capsys):
    main()
    captured = capsys.readouterr()
    assert "development environment is ready" in captured.out
