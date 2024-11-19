def delivery_report(err, msg):
    if err is None:
        print('Message delivered to topic %s partition [%d] @ offset %d' % (msg.topic(), msg.partition(), msg.offset()))      
    else:
        print('Error delivering message: %s' % err)
