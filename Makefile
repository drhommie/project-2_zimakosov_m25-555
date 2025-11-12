install:
	poetry install

run:
	poetry run database

build:
	poetry build

publish:
	poetry publish --dry-run

package-install:
	python3 -m pip install --force-reinstall "$$(ls -t dist/*.whl | head -n1)"

lint:
	poetry run ruff check .




