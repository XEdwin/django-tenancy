from __future__ import unicode_literals

from south.db import db
from south.v2 import DataMigration, SchemaMigration

from .. import get_tenant_model


class TenantMigrationMixin(object):
    def pre_tenant_step_sql(self, tenant, default_schema_name):
        return ["SET search_path TO %s, %s" % (
            tenant.db_schema, default_schema_name
        )]

    def post_tenant_step_sql(self, tenant, default_schema_name):
        return ["SET search_path TO %s" % default_schema_name]

    def tenant_step(self, orm, operation):
        default_schema_name = db.default_schema_name
        deferred_sql = db.deferred_sql
        for tenant in get_tenant_model()._default_manager.all():
            # Set the default schema name to the tenant one in order to
            # allow constraints retreival.
            db.default_schema_name = tenant.db_schema

            pre_sql = self.pre_tenant_step_sql(tenant, default_schema_name)
            for statement in pre_sql:
                db.execute(statement)
            pre_deferred_len = len(deferred_sql)

            operation(tenant, orm)

            post_deferred_len = len(deferred_sql)
            post_sql = self.post_tenant_step_sql(tenant, default_schema_name)
            for statement in post_sql:
                db.execute(statement)
            # If some deferred statements were added we wrap them with the
            # required pre/post sql statements.
            if post_deferred_len > pre_deferred_len:
                deferred_sql[pre_deferred_len:pre_deferred_len] = pre_sql
                deferred_sql.extend(post_sql)

            # Set back the default db shema
            db.default_schema_name = default_schema_name

    def forwards(self, orm):
        self.tenant_step(orm, self.tenant_forwards)

    def backwards(self, orm):
        self.tenant_step(orm, self.tenant_backwards)

    def tenant_forwards(self, account, orm):
        raise NotImplementedError

    def tenant_backwards(self, account, orm):
        raise NotImplementedError


class TenantSchemaMigration(TenantMigrationMixin, SchemaMigration):
    pass


class TenantDataMigration(TenantMigrationMixin, DataMigration):
    pass
