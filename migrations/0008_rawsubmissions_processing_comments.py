# Generated by Django 2.2.6 on 2021-07-29 05:27

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('vendor', '0007_auto_20200825_0328'),
    ]

    operations = [
        migrations.AddField(
            model_name='rawsubmissions',
            name='processing_comments',
            field=models.CharField(blank=True, max_length=10000, null=True),
        ),
    ]
