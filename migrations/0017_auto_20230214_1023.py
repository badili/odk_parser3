# Generated by Django 3.2.1 on 2023-02-14 10:23

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('vendor', '0016_auto_20230214_1014'),
    ]

    operations = [
        migrations.AlterField(
            model_name='odkformgroup',
            name='comments',
            field=models.CharField(blank=True, max_length=1000, null=True),
        ),
        migrations.AlterField(
            model_name='odkformgroup',
            name='order_index',
            field=models.SmallIntegerField(blank=True, null=True),
        ),
    ]
