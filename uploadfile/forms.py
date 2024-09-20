import os.path

from django import forms

class FileUploadForm(forms.Form):
    client_name = forms.CharField(max_length=225)
    file = forms.FileField(required=True)

    def file(self):
        upload_file = self.cleaned_data['file']
        valid_extensions = ['.csv','.xlsx','.xls']
        file_extension = os.path.splitext(upload_file.name)[1].lower()
        if file_extension not in valid_extensions:
            raise forms.ValidationError("Only Csv and Excel files are allowed.")
        return upload_file