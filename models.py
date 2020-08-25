from django.contrib.auth.models import User
from django.db import models
from django.db.models.signals import post_save
from django.dispatch import receiver
from django.core.validators import RegexValidator, MaxLengthValidator, MinLengthValidator
from django.contrib.auth import get_user_model

from easy_thumbnails.fields import ThumbnailerImageField
from django.conf import settings

if settings.DATABASES['default']['ENGINE'] == 'django.db.backends.mysql':
    # from django_mysql.models import JSONField
    from jsonfield import JSONField
elif settings.DATABASES['default']['ENGINE'] == 'django.db.backends.postgresql_psycopg2':
    from django.contrib.postgres.fields import JSONField


class Profile(models.Model):
    """
    The Profile class that adds attributes to the default auth user.
    """
    # Remove the base_dir from the profile photo directory
    profile_photo_dir = settings.PROFILE_PHOTO_DIR
    base_dir = settings.BASE_DIR
    profile_photo_dir = profile_photo_dir.replace(base_dir + '/', '').strip()

    # user = models.OneToOneField(User, on_delete=models.PROTECT)
    user = get_user_model()
    photo = ThumbnailerImageField(upload_to=profile_photo_dir, blank=True)
    salutation = models.CharField(max_length=20, blank=True, default='Dear')
    phone = models.CharField(max_length=20, blank=True, default='')
    city = models.CharField(max_length=60, default='', blank=True)
    country = models.CharField(max_length=100, default='', blank=True)
    organization = models.CharField(max_length=100, default='', blank=True)
    bio = models.TextField(max_length=100, default='', blank=True)
    profession = models.CharField(max_length=50, blank=True)

    class Meta:
        verbose_name = 'profile'
        db_table = 'profile'
        verbose_name_plural = 'profiles'

    def save(self, *args, **kwargs):
        if not self.pk:
            try:
                p = Profile.objects.get(user=self.user)
                self.pk = p.pk
            except Profile.DoesNotExist:
                pass

        super(Profile, self).save(*args, **kwargs)

    def __str__(self):
        return self.user.username


@receiver(post_save, sender=User)
def create_or_update_user_profile(sender, instance, created, **kwargs):
    print(instance)
    if created:
        Profile.objects.create(user=instance)
    # instance.profile.save()


class BaseTable(models.Model):
    """
    Base abstract table to be inherited by all other tables
    """
    date_created = models.DateTimeField(auto_now=True)
    date_modified = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True


class GeneralPermissions(models.Model):
    """
    A class to enable me add permissions not tied to any model.
    """

    class Meta:
        permissions = (
            ('view_analysis', 'View Analysis'),
            ('view_gallery', 'Can view gallery')
        )


class Model(BaseTable):
    """
    Defines the structure of the model table
    """
    # the columns for the tables
    model_name = models.CharField(max_length=100)

    class Meta:
        db_table = '__models'

    def publish(self):
        self.save()


class Attribute(BaseTable):
    """
    Defines the structure of the attributes table
    """
    _name = models.CharField(max_length=100)
    _type = models.CharField(max_length=50)
    _size = models.SmallIntegerField()
    _model = models.ForeignKey('Model', on_delete=models.PROTECT)

    class Meta:
        db_table = '__attributes'

    def publish(self):
        self.save()


class ODKFormGroup(BaseTable):
    # define the dictionary structure
    order_index = models.SmallIntegerField(null=True)
    group_name = models.CharField(max_length=100, unique=True)
    comments = models.CharField(max_length=1000, null=True)

    class Meta:
        db_table = 'form_groups'

    def publish(self):
        self.save()

    def get_id(self):
        return self.id


class SystemSettings(BaseTable):
    # system settings table
    setting_name = models.CharField(max_length=200)
    setting_key = models.CharField(max_length=100, unique=True)
    setting_value = models.CharField(max_length=1000)
    parent_id = models.CharField(max_length=20, null=True, blank=True)      # an optional placeholder to link to a parent model, not necessary in this app

    class Meta:
        db_table = 'system_settings'

    def publish(self):
        self.save()

    def get_id(self):
        return self.id


class ODKForm(BaseTable):
    # Define the structure of the form table
    form_id = models.IntegerField(unique=True, db_index=True)
    form_group = models.ForeignKey(ODKFormGroup, null=True, on_delete=models.PROTECT)
    form_name = models.CharField(max_length=200, unique=True)
    full_form_id = models.CharField(max_length=200, unique=True)
    structure = JSONField(null=True)
    processed_structure = JSONField(null=True)
    auto_update = models.BooleanField(default=False)
    is_source_deleted = models.BooleanField(default=False)
    no_submissions = models.SmallIntegerField(default=0)
    is_active = models.BooleanField(default=0)
    datetime_published = models.DateTimeField(default=None)
    latest_upload = models.DateTimeField(default=None)


    class Meta:
        db_table = 'odkform'
        permissions = (
            ('view_odk_form', 'View ODKForm'),
            ('download_odk_form', 'Download ODKForm')
        )

    def publish(self):
        self.save()

    def get_id(self):
        return self.id


class RawSubmissions(BaseTable):
    # Define the structure of the submission table
    form = models.ForeignKey(ODKForm, on_delete=models.PROTECT)
    uuid = models.CharField(max_length=100, unique=True, db_index=True)
    submission_time = models.CharField(max_length=100)
    is_processed = models.SmallIntegerField(default=0)
    is_modified = models.BooleanField(default=0)
    raw_data = JSONField()

    class Meta:
        db_table = 'raw_submissions'
        permissions = (
            ('view_raw_submissions', 'View RawSubmissions'),
            ('download_raw_submissions', 'Download RawSubmissions')
        )

    def publish(self):
        self.save()

    def get_id(self):
        return self.uuid


class FormViews(BaseTable):
    # Define the structure of the submission table
    form = models.ForeignKey(ODKForm, on_delete=models.PROTECT)
    view_name = models.CharField(max_length=100, unique=True, db_index=True)
    proper_view_name = models.CharField(max_length=100, db_index=True)
    structure = JSONField()

    class Meta:
        db_table = 'form_views'
        permissions = (
            ('view_form_views', 'View FormViews'),
            ('download_form_views', 'Download FormViews')
        )

    def publish(self):
        self.save()

    def get_id(self):
        return self.view_name


class ViewTablesLookup(BaseTable):
    # Define the structure of the views that will be generated
    view = models.ForeignKey(FormViews, on_delete=models.PROTECT)
    table_name = models.CharField(max_length=250, unique=True, db_index=True)
    proper_table_name = models.CharField(max_length=250, null=True, db_index=True)
    hashed_name = models.CharField(max_length=100, unique=True, db_index=True)

    class Meta:
        db_table = 'views_table_lookup'

    def publish(self):
        self.save()

    def get_id(self):
        return self.table_name


class ViewsData(BaseTable):
    # Define the structure of the submission table
    view = models.ForeignKey(FormViews, on_delete=models.PROTECT)
    raw_data = JSONField()

    class Meta:
        db_table = 'views_data'

    def publish(self):
        self.save()

    def get_id(self):
        return self.view


class ImagesLookup(models.Model):
    # Define the structure of the submission table
    filename = models.CharField(max_length=50, unique=True, db_index=True)
    species = models.CharField(max_length=50, null=True)
    breed = models.CharField(max_length=50, null=True)
    country = models.CharField(max_length=80, null=True)

    class Meta:
        db_table = 'images_lookup'

    def publish(self):
        self.save()

    def get_id(self):
        return self.filename


class DictionaryItems(BaseTable):
    # define the dictionary structure
    form_group = models.CharField(max_length=100, db_index=True)
    parent_node = models.CharField(max_length=100, db_index=True, null=True)
    t_key = models.CharField(max_length=100, db_index=True)
    t_locale = models.CharField(max_length=50)
    t_type = models.CharField(max_length=30, db_index=True)
    t_value = models.CharField(max_length=1000)

    class Meta:
        unique_together = ('form_group', 'parent_node', 't_key')
        db_table = 'dictionary_items'

    def publish(self):
        self.save()

    def get_id(self):
        return self.t_key


class Country(models.Model):
    """
    This model holds information for a given country such as, population,
    population or map polygon.
    """
    id = models.AutoField(primary_key=True)
    iso_code = models.CharField(max_length=10)
    name = models.CharField(max_length=50)
    polygon = models.TextField()
    center_lat = models.FloatField()
    center_long = models.FloatField()
    
    def __repr__(self):
        return "Country <{}>".format(self.name)

    def __str__(self):
        return self.name


class FormMappings(BaseTable):
    # define the dictionary structure
    form_group = models.CharField(max_length=50, db_index=True)
    form_question = models.CharField(max_length=100)
    dest_table_name = models.CharField(max_length=100, db_index=True)
    dest_column_name = models.CharField(max_length=50, db_index=True)
    # @todo Add proper handling of choices like the enum in mysql
    odk_question_type = models.CharField(max_length=50)     # This will be, single_select, multiple_select, integer,
    db_question_type = models.CharField(max_length=50)     # This will be, single_select, multiple_select, integer,
    # @todo change this to a proper validator field
    ref_table_name = models.CharField(max_length=100, null=True)
    ref_column_name = models.CharField(max_length=50, null=True)
    validation_regex = models.CharField(max_length=100, null=True)
    is_record_identifier = models.BooleanField(default=False)
    is_null = models.SmallIntegerField(null=True)
    is_lookup_field = models.NullBooleanField(default=False, null=True)
    use_current_time = models.NullBooleanField(default=False, null=True)

    class Meta:
        unique_together = ('form_group', 'form_question', 'dest_table_name', 'dest_column_name')
        db_table = 'mappings_table'

    def publish(self):
        self.save()

    def get_id(self):
        return self.t_key


class ProcessingErrors(BaseTable):
    # define the dictionary structure
    err_code = models.IntegerField(db_index=True)
    err_message = models.TextField()
    data_uuid = models.CharField(max_length=100, db_index=True, unique=True)
    err_comments = models.CharField(max_length=1000, null=True)
    is_resolved = models.BooleanField(default=False)

    class Meta:
        # unique_together = ('form_group', 'form_question', 'dest_table_name', 'dest_column_name')
        db_table = 'processing_errors'

    def publish(self):
        self.save()

    def get_id(self):
        return self.t_key
