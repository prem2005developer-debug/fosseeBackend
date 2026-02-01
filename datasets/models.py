from django.db import models

class Dataset(models.Model):
    name = models.CharField(max_length=255)
    raw_data = models.TextField(null=True, blank=True)
    summary = models.TextField(null=True, blank=True)
    uploaded_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return self.name
