# Generated by Django 2.2.6 on 2020-01-29 14:56

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('vendor', '0004_auto_20200101_1944'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='profile',
            name='user',
        ),
    ]
