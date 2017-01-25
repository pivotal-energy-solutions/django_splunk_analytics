# Django Splunk Analytics

Analytics Framework for Django and Splunk

## Problem: ##
How do you extract out analytical information which you can use upstream?

## Our Approach ##

- As events happen we want a process to dump the corresponding data in json format to a flat file.  This will then be
  used by a SplunkForwarder so that it can be sent upstream.


Each type of information will be put in it's own file (source)

## Considerationss ##
1.  Historical Catchup
2.  Celery Jobs to Dispatch - Low priority
3.  Using the serializer built in DRF?

