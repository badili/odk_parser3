# Generated by Django 2.2.6 on 2021-11-01 01:38

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('vendor', '0008_rawsubmissions_processing_comments'),
    ]

    operations = [
        migrations.AddField(
            model_name='rawsubmissions',
            name='duration',
            field=models.PositiveIntegerField(blank=True, default=0, null=True),
        ),
        migrations.AddField(
            model_name='rawsubmissions',
            name='instance_id',
            field=models.PositiveIntegerField(blank=True, default=0, null=True),
        ),
    ]
