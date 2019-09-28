from django.contrib.postgres.fields import ArrayField
from django.core.validators import RegexValidator
from django.db import models

from works_single_view.utils import ISWC_REGEX


class MusicalWork(models.Model):
    title = models.CharField(max_length=1024, null=True, blank=True)
    contributors = ArrayField(base_field=models.CharField(max_length=255), null=True)
    iswc = models.CharField(
        max_length=15,
        null=True,
        blank=True,
        validators=[RegexValidator(ISWC_REGEX)],
        unique=True,
    )

    class Meta:
        db_table = "musical_works"
