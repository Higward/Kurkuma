from kedro.io.core import AbstractDataSet, DataSetError
from kedro.io.core import DataSetError

import json
from urllib.parse import urlsplit, urlunsplit
import requests
import socket
from typing import Any, Dict, Tuple, Union
from requests.auth import AuthBase


class APIWithTokenDataSet(AbstractDataSet):
    """
    `APIWithTokenDataSet` loads the data from HTTP(S) APIs by receiving and using the generated token.
        Example:
    ::
        >>> data_set = APIDataSet(
        >>>     url = "https://somesite.com/somepath"
        >>>     credentials = "{username:username, password:password}"
        >>>     method = "get"
        >>> )
        >>> data = data_set.load()
    """

    # pylint: disable=too-many-arguments
    def __init__(
            self,
            url: str,
            method: str = "GET",
            data: Any = None,
            params: Dict[str, Any] = None,
            auth: Union[Tuple[str], AuthBase] = None,
            timeout: int = 60,
            credentials: Dict[str, Any] = None,
    ) -> None:
        """Creates a new instance of ``APIDataSet`` to fetch data from an API endpoint.

        Args:
            url: The API URL endpoint.
            method: The Method of the request, GET, POST, PUT, DELETE, HEAD, etc...
            data: The request payload, used for POST, PUT, etc requests
                https://requests.readthedocs.io/en/master/user/quickstart/#more-complicated-post-requests
            params: The url parameters of the API.
                https://requests.readthedocs.io/en/master/user/quickstart/#passing-parameters-in-urls
            auth: Anything ``requests`` accepts. Normally it's either ``('login', 'password')``,
                or ``AuthBase``, ``HTTPBasicAuth`` instance for more complex cases.
            timeout: The wait time in seconds for a response, defaults to 1 minute.
                https://requests.readthedocs.io/en/master/user/quickstart/#timeouts
            credentials: Secret login and password for generating token
        """
        super().__init__()
        self._request_args: Dict[str, Any] = {
            "url": url,
            "method": method,
            "data": data,
            "params": params,
            "auth": auth,
            "timeout": timeout,
        }
        self.credentials = credentials

    def _execute_request(self) -> requests.Response:
        try:
            login_password = self.credentials
            connection_url = self._request_args['url']
            token_url = urlunsplit((urlsplit(connection_url).scheme, urlsplit(connection_url).netloc, "token", "", ""))
            token_data = json.loads(requests.post(url=token_url, data=login_password, verify=True).content)

            response = requests.request(**self._request_args, headers={
                "Authorization": ' '.join((token_data['token_type'], token_data['access_token']))})
            response.raise_for_status()
        except requests.exceptions.HTTPError as exc:
            raise DataSetError("Failed to fetch data", exc)
        except socket.error:
            raise DataSetError("Failed to connect to the remote server")

        return response

    def _load(self) -> requests.Response:
        return self._execute_request()

    def _save(self, data: Any) -> None:
        raise DataSetError(
            "{} is a read only data set type".format(self.__class__.__name__)
        )

    def _exists(self) -> bool:
        response = self._execute_request()

        return response.ok

    def _describe(self) -> Dict[str, Any]:
        return dict(**self._request_args)
