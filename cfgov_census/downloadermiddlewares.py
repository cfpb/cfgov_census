
class CFGOVMethodSelectingMiddleware():
    def process_request(self, request, spider):
        if 'files.consumerfinance.gov' in request.url or\
           's3.amazonaws.com' in request.url:
            if request.url.endswith('robots.txt') or request.method == 'HEAD':
                return
            return request.replace(method='HEAD')

        return None
