0 0 * * *   export PYTHONPATH='/mnt/sda1/opkg/usr/lib/python2.7/site-packages:/mnt/sda1/opkg/usr/lib/python2.7/lib-dynload/';\
            /usr/bin/python /root/home-sensors/data-upload/upload-to-s3.py 2>> /root/cron.err
