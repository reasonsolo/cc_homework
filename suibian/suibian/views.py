from django.shortcuts import render_to_response
from django.template import RequestContext

def home(request):
    kwords = request.GET.get('kwords', '')
    if len(kwords) == 0:
        return render_to_response('home.html', {
            'kwords': kwords,
            }, context_instance = RequestContext(request))
    else:
        return render_to_response('search.html', {
            'kwords': kwords,
            }, context_instance = RequestContext(request))
