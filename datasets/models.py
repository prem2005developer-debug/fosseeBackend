from django.db import models

class Dataset(models.Model):
    name = models.CharField(max_length=255)
    raw_data = models.JSONField(null=True, blank=True)
    summary = models.JSONField(null=True, blank=True)
    uploaded_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return self.name