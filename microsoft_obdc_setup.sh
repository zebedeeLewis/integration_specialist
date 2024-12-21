case $(uname -m) in
  x86_64) architecture="amd64" ;;
  arm64 ) architecture="arm64" ;;
  *     ) architecture="unsupported" ;;
esac
if [[ "unsupported" == "$architecture" ]];
then
    echo "Alpine architecture $(uname -m) is not currently supported.";
    exit;
fi

#Download the desired package(s)
curl -O "https://download.microsoft.com/download/7/6/d/76de322a-d860-4894-9945-f0cc5d6a45f8/msodbcsql18_18.4.1.1-1_$architecture.apk" 2> /dev/null
curl -O "https://download.microsoft.com/download/7/6/d/76de322a-d860-4894-9945-f0cc5d6a45f8/mssql-tools18_18.4.1.1-1_$architecture.apk" 2> /dev/null

#(Optional) Verify signature, if 'gpg' is missing install it using 'apk add gnupg':
curl -O "https://download.microsoft.com/download/7/6/d/76de322a-d860-4894-9945-f0cc5d6a45f8/msodbcsql18_18.4.1.1-1_$architecture.sig" 2> /dev/null
curl -O "https://download.microsoft.com/download/7/6/d/76de322a-d860-4894-9945-f0cc5d6a45f8/mssql-tools18_18.4.1.1-1_$architecture.sig" 2> /dev/null

curl "https://packages.microsoft.com/keys/microsoft.asc" 2> /dev/null  | gpg --import -
gpg --verify "msodbcsql18_18.4.1.1-1_$architecture.sig" "msodbcsql18_18.4.1.1-1_$architecture.apk"
gpg --verify "mssql-tools18_18.4.1.1-1_$architecture.sig" "mssql-tools18_18.4.1.1-1_$architecture.apk"

#Install the package(s)
apk add --allow-untrusted "msodbcsql18_18.4.1.1-1_$architecture.apk"
apk add --allow-untrusted "mssql-tools18_18.4.1.1-1_$architecture.apk"
