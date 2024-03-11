from typing import Iterable

import json
import datahub.emitter.mce_builder as builder
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (
    SourceReport,
    Source,
)

from datahub.metadata.schema_classes import DomainPropertiesClass, ChangeTypeClass
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit


from .config import CreateDerivedTableDomainsConfig


@config_class(CreateDerivedTableDomainsConfig)
class CreateDerivedTableDomainsSource(Source):
    source_config: CreateDerivedTableDomainsConfig
    report: SourceReport = SourceReport()

    def __init__(self, config: CreateDerivedTableDomainsConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.source_config = config

    @classmethod
    def create(cls, config_dict, ctx):
        config = CreateDerivedTableDomainsConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        if self.source_config.manifest_s3_uri is None:
            with open(self.source_config.manifest_local_path, "r") as f:
                manifest = json.load(f)
        else:
            # TODO: implement get from s3
            pass
        for domain_name in self._get_domains(manifest):
            mcp = self._make_domain(domain_name)
            wu = MetadataWorkUnit("single_mcp", mcp=mcp)
            self.report.report_workunit(wu)

            yield wu

    def _get_domains(self, manifest) -> set:
        domains = set()
        for k, v in manifest["nodes"].items():
            (
                domains.add(manifest["nodes"][k]["fqn"][1])
                if not manifest["nodes"][k]["resource_type"] == "seed"
                else None
            )
        return domains

    def _make_domain(self, domain_name) -> MetadataChangeProposalWrapper:
        domain_urn = builder.make_domain_urn(domain=domain_name)
        domain_properties = DomainPropertiesClass(name=domain_name)
        metadata_event = MetadataChangeProposalWrapper(
            entityType="domain",
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=domain_urn,
            aspect=domain_properties,
        )
        return metadata_event

    def get_report(self) -> SourceReport:
        return self.report

    def close(self) -> None:
        pass
