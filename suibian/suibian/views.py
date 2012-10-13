from django.shortcuts import render_to_response
from django.template import RequestContext
from search import search

def home(request):
    kwords = request.GET.get('kwords', '')
    if len(kwords) == 0:
        return render_to_response('home.html', {
            'kwords': kwords,
            }, context_instance = RequestContext(request))
    else:
        kwords_lst = kwords.split()
        results = search(kwords_lst)
        return render_to_response('search.html', {
            'kwords': kwords,
            'results': results,
            }, context_instance = RequestContext(request))
