from create_derived_table_domains_source.config import CreateDerivedTableDomainsConfig
from create_derived_table_domains_source.source import CreateDerivedTableDomainsSource

from datahub.ingestion.api.common import PipelineContext


def test_ingest():
    source = CreateDerivedTableDomainsSource(
        ctx=PipelineContext(run_id="domain-source-test"),
        config=CreateDerivedTableDomainsConfig(
            manifest_local_path="tests/data/manifest.json"
        ),
    )

    results = list(source.get_workunits())

    assert results

    domains = [result.metadata.aspect.name for result in results]
    domains.sort()

    assert domains == ["courts", "hq", "prison", "probation"]
