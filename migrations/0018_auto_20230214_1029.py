# Generated by Django 3.2.1 on 2023-02-14 10:29

from django.db import migrations, models
import jsonfield.fields


class Migration(migrations.Migration):

    dependencies = [
        ('vendor', '0017_auto_20230214_1023'),
    ]

    operations = [
        migrations.AlterField(
            model_name='odkform',
            name='latest_upload',
            field=models.DateTimeField(blank=True, default=None, null=True),
        ),
        migrations.AlterField(
            model_name='odkform',
            name='processed_structure',
            field=jsonfield.fields.JSONField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='odkform',
            name='structure',
            field=jsonfield.fields.JSONField(blank=True, null=True),
        ),
    ]
