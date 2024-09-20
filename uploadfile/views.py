from django.shortcuts import render, HttpResponseRedirect
from .forms import FileUploadForm
import os
from django.conf import settings
import boto3


# Create your views here.
def upload_file(request):
    form = FileUploadForm()
    success_message = ''
    error_message = ''
    if request.method == 'POST':
        form = FileUploadForm(request.POST, request.FILES)
        if form.is_valid():
            client_name = form.cleaned_data['client_name']
            uploaded_file = request.FILES.get('file')

            if uploaded_file:

                uploaded_file_name, uploaded_file_extension = os.path.splitext(uploaded_file.name)
                valid_extensions = ['.csv', '.xls', '.xlsx']
                if uploaded_file_extension.lower() in valid_extensions:
                    s3 = boto3.client('s3', aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
                                      aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
                                      region_name=settings.AWS_S3_REGION_NAME)
                    # Upload the file to S3
                    s3.upload_fileobj(uploaded_file, settings.AWS_STORAGE_BUCKET_NAME, uploaded_file.name)

                    success_message = 'File uploaded successfully!'
                    return HttpResponseRedirect('upload_success/')
                else:
                    error_message = "Invalid file. Only CSV and Excel files are allowed"
            else:
                error_message = " No file uploaded"
        else:
            error_message = "Form submission failed. Please check your input."

    return render(request, 'upload_form.html',
                  {'form': form, 'success_message': success_message, 'error_message': error_message})


def upload_success(request):
    return render(request, 'upload_success.html')