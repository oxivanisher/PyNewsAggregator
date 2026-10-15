self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', e => e.waitUntil(clients.claim()));

self.addEventListener('push', event => {
    const data = event.data ? event.data.json() : {};
    event.waitUntil(
        self.registration.showNotification(data.title || '📰 News', {
            body: data.body || 'New articles are available.',
            icon: '/static/icon-192.png',
            badge: '/static/badge-96.png',  // monochrome: Android only uses alpha
            tag: 'new-articles',  // replaces any existing notification silently
            data: { url: '/' },
        })
    );
});

// The installed app launches with ?source=pwa (manifest start_url), which lets us
// tell its window apart from ordinary browser tabs on the same origin.
const APP_URL = '/?source=pwa';

self.addEventListener('notificationclick', event => {
    event.notification.close();
    event.waitUntil(
        clients.matchAll({ type: 'window', includeUncontrolled: true }).then(list => {
            for (const c of list) {
                if (new URL(c.url).searchParams.get('source') === 'pwa' && 'focus' in c) return c.focus();
            }
            // Don't focus a browser tab: on Android an in-scope openWindow launches
            // the installed app instead (or a new tab if the app isn't installed).
            return clients.openWindow(APP_URL);
        })
    );
});
