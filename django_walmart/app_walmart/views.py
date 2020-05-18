from django.shortcuts import render
from django.http import HttpResponse
import json
# Create your views here.
def index(request):
    json_object = {}
    with open("details.json", 'r') as openfile:
        json_object = json.load(openfile)
    return render(request,'index.html',context=json_object)
