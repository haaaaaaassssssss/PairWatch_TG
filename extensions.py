import os



class ProxyAuthExtension:
    manifest_json = """
    {
        "version": "1.0.0",
        "manifest_version": 2,
        "name": "Chrome Proxy",
        "permissions": [
            "proxy",
            "tabs",
            "unlimitedStorage",
            "storage",
            "<all_urls>",
            "webRequest",
            "webRequestBlocking"
        ],
        "background": {"scripts": ["background.js"]},
        "minimum_chrome_version": "76.0.0"
    }
    """
    background_js = """
    var config = {
        mode: "fixed_servers",
        rules: {
            singleProxy: {
                scheme: "http",
                host: "%s",
                port: %d
            },
            bypassList: ["localhost"]
        }
    };

    chrome.proxy.settings.set({value: config, scope: "regular"}, function() {});

    function callbackFn(details) {
        return {
            authCredentials: {
                username: "%s",
                password: "%s"
            }
        };
    }

    chrome.webRequest.onAuthRequired.addListener(
        callbackFn,
        { urls: ["<all_urls>"] },
        ['blocking']
    );
    """

    def __init__(self, host, port, user, password,proxy_dirname):
        self._dir = os.path.join(os.getcwd(), "Extensions", proxy_dirname)
        os.makedirs(self._dir, exist_ok=True)

        self.manifest_file = os.path.join(self._dir, "manifest.json")
        with open(self.manifest_file, "w") as f:
            f.write(self.manifest_json)

        background_js = self.background_js % (host, port, user, password)
        self.background_file = os.path.join(self._dir, "background.js")
        with open(self.background_file, "w") as f:
            f.write(background_js)

    @property
    def directory(self):
        return self._dir