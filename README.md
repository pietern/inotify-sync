# inotify-sync

Monitor directory using inotify; publish new files through Redis.

I built this to synchronize a ccache directory across machines without
having to rely on NFS. You can use this for other stuff as well
though, but keep in mind that this doesn't give you any consistency
guarantees.
