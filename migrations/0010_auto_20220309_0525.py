# Generated by Django 3.2.1 on 2022-03-09 05:25

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('vendor', '0009_auto_20211101_0138'),
    ]

    operations = [
        migrations.AlterField(
            model_name='formmappings',
            name='is_lookup_field',
            field=models.BooleanField(default=False, null=True),
        ),
        migrations.AlterField(
            model_name='formmappings',
            name='use_current_time',
            field=models.BooleanField(default=False, null=True),
        ),
    ]
