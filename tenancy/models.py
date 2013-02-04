from __future__ import unicode_literals
from collections import namedtuple
import copy

import django
from django.contrib.contenttypes.models import ContentType
from django.db import connections, models
from django.db.models.fields.related import RelatedField
from django.utils.datastructures import SortedDict

from . import get_tenant_model


class AbstractTenant(models.Model):
    class Meta:
        abstract = True

    @property
    def db_schema(self):
        raise NotImplementedError


class Tenant(AbstractTenant):
    name = models.CharField(unique=True, max_length=20)

    class Meta:
        if django.VERSION >= (1, 5):
            swappable = 'TENANCY_TENANT_MODEL'

    @property
    def db_schema(self):
        return "tenant_%s" % self.name


class TenantOptions(object):
    def __init__(self, related_name, related_fields):
        self.related_name = related_name
        self.related_fields = related_fields

    def related_fields_for_tenant(self, tenant, opts):
        fields = {}
        for fname, related_field in self.related_fields.items():
            field = copy.deepcopy(related_field)
            rel_to = related_field.rel.to
            if isinstance(rel_to, basestring):
                rel_to = TenantModelBase.instances[rel_to].model
            related_name = rel_to._tenant_meta.related_name
            field.rel.to = getattr(tenant, related_name).model
            if (isinstance(field, models.ManyToManyField) and
                not field.rel.through):
                if field.name is None:
                    field.name = fname
                field.db_table = db_schema_table(tenant, field._get_m2m_db_table(opts))
            fields[fname] = field
        return fields


def meta(**opts):
    """
    Create a class with specified opts as attributes to be used as model
    definition options.
    """
    return type(str('Meta'), (), opts)


def db_schema_table(tenant, db_table):
    connection = connections[tenant._state.db]
    if connection.vendor == 'postgresql':
        # See https://code.djangoproject.com/ticket/6148#comment:47
        return '%s\".\"%s' % (tenant.db_schema, db_table)
    else:
        return "%s_%s" % (tenant.db_schema, db_table)


Reference = namedtuple('Reference', ['related_name', 'model'])


class TenantModelBase(models.base.ModelBase):
    instances = SortedDict()

    def __new__(cls, name, bases, attrs):
        super_new = super(TenantModelBase, cls).__new__
        related_fields = {}
        for key, value in attrs.items():
            if isinstance(value, RelatedField):
                rel_to = value.rel.to
                if isinstance(rel_to, basestring):
                    if rel_to in cls.instances:
                        related_fields[key] = attrs.pop(key)
        Meta = attrs.setdefault('Meta', meta())
        # It's not an abstract model
        if not getattr(Meta, 'abstract', False):
            Meta.abstract = True
            module = attrs.get('__module__')
            # Create the abstract model to be returned.
            model = super_new(cls, name, bases, attrs)
            opts = model._meta
            # Extract the specified related name if it exists.
            try:
                related_name = model.TenantMeta.related_name
            except AttributeError:
                related_name = name.lower() + 's'
            # Store instances in order to reference them with related fields
            cls.instances["%s.%s" % (opts.app_label, opts.object_name)] = Reference(related_name, model)
            # Attach a descriptor to the tenant model to access the underlying
            # model based on the tenant instance.
            def type_(tenant, **attrs):
                attrs.update(
                    tenant=tenant,
                    __module__=module
                )
                type_bases = [model]
                for base in bases:
                    if isinstance(base, cls):
                        base_tenant_opts = base._tenant_meta
                        # Add related tenant fields of the base
                        attrs.update(base_tenant_opts.related_fields_for_tenant(tenant, opts))
                        base_related_name = base_tenant_opts.related_name
                        if base_related_name:
                            type_bases.append(getattr(tenant, base_related_name).model)
                            continue
                    type_bases.append(base)
                # Add related tenant fields of the abstract base
                attrs.update(model._tenant_meta.related_fields_for_tenant(tenant, opts))
                return super_new(cls, name, tuple(type_bases), attrs)
            descriptor = TenantModelDescriptor(type_, opts)
            tenant_model = get_tenant_model(model._meta.app_label)
            setattr(tenant_model, related_name, descriptor)
        else:
            related_name = None
            model = super_new(cls, name, bases, attrs)
        model._tenant_meta = TenantOptions(related_name, related_fields)
        return model


class TenantModelDescriptor(object):
    def __init__(self, type_, opts):
        self.type = type_
        self.opts = opts

    def app_label(self, tenant):
        return "tenant_%s_%s" % (tenant.pk, self.opts.app_label)

    def natural_key(self, tenant):
        return (self.app_label(tenant), self.opts.module_name)

    def __get__(self, instance, owner):
        if not instance:
            return self
        try:
            natural_key = self.natural_key(instance)
            content_type = ContentType.objects.get_by_natural_key(*natural_key)
        except ContentType.DoesNotExist:
            # We must create the content type and the model class
            content_type = model_class = None
        else:
            # Attempt to retrieve the model class from the content type.
            # At this point, the model class can be None if it's cached yet.
            model_class = content_type.model_class()
        if model_class is None:
            # The model class has not been created yet, we define it.
            # TODO: Use `db_schema` once django #6148 is fixed.
            db_table = db_schema_table(instance, self.opts.db_table)
            model_class = self.type(
                tenant=instance,
                Meta=meta(app_label=self.app_label(instance), db_table=db_table)
            )
            # Make sure to create the content type associated with this model
            # class that was just created.
            if content_type is None:
                ContentType.objects.get_for_model(model_class)
        return model_class._default_manager


class TenantModel(models.Model):
    __metaclass__ = TenantModelBase

    class Meta:
        abstract = True
