# Sharing Eventflow

Run the source twice against a fresh local database before publishing a change to replay behavior:

```bash
python -m pip install -e . -r requirements-dev.txt
python -m eventflow run --input streams/stream.jsonl --config streams/default.json --database artifacts/publish-check.sqlite --output artifacts/first.json
python -m eventflow run --input streams/stream.jsonl --config streams/default.json --database artifacts/publish-check.sqlite --output artifacts/replayed.json
python dev/check_recovery.py --require-lint --wheel
```

The fixture should report ten first commits and then eleven duplicate inputs, with unchanged sink/checkpoint state. Keep the database out of the source distribution. The portable evidence is the source fixture, policy, tests and JSON histories; the database is a mutable checkpoint tied to one run.

Review `protocol/watermark-contract.md` if you change lateness, progress or capacity defaults. Existing checkpoint configurations are checked exactly, so such a change needs an explicit new database or application migration plan. Updating a README example alone must not imply that an old database can be reinterpreted.

For a ZIP checkout, initialize Git and commit the reviewed source before adding an empty remote. The workspace copy already has its own Git metadata.

```bash
git remote add origin https://github.com/YOUR_USERNAME/eventflow.git
git push -u origin HEAD
```

CI calls `dev/check_recovery.py`, including installation outside the checkout, and stores `reports/verification.json`. Its hosted status is available after pushing. Publishing this local processor does not supply a Kafka acknowledgement or remote sink transaction protocol.
