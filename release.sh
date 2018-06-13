#!/bin/sh
echo 'packaging files ...'
# release
cd ~/insiderpy/lib/python3.6/site-packages/
zip -r9 ~/insider.$1.zip *
cd ~/th3sys/insider
zip -g ~/insider.$1.zip check.py
zip -g ~/insider.$1.zip companies.py
zip -g ~/insider.$1.zip connectors.py
zip -g ~/insider.$1.zip find.py
zip -g ~/insider.$1.zip save.py
zip -g ~/insider.$1.zip trading.py
zip -g ~/insider.$1.zip utils.py
aws s3 cp ~/insider.$1.zip s3://$2/releases/insider.$1.zip