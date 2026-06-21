self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', e => e.waitUntil(clients.claim()));

self.addEventListener('push', event => {
    const data = event.data ? event.data.json() : {};
    event.waitUntil(
        self.registration.showNotification(data.title || '📰 News', {
            body: data.body || 'New articles are available.',
            icon: '/static/icon.svg',
            badge: '/static/icon.svg',
            tag: 'new-articles',  // replaces any existing notification silently
            data: { url: '/' },
        })
    );
});

self.addEventListener('notificationclick', event => {
    event.notification.close();
    event.waitUntil(
        clients.matchAll({ type: 'window', includeUncontrolled: true }).then(list => {
            for (const c of list) {
                if (c.url.startsWith(self.location.origin) && 'focus' in c) return c.focus();
            }
            return clients.openWindow('/');
        })
    );
});
