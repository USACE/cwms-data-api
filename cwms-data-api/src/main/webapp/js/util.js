function load (id, url) {
	var con = document.getElementById(id)
	var xhr = new XMLHttpRequest();

	xhr.onreadystatechange = function (e) { 
		if (xhr.readyState == 4 && xhr.status == 200) {
			con.innerHTML = xhr.responseText;
		}
	}

	xhr.open("GET", url, true);
	xhr.setRequestHeader('Content-type', 'text/html');
	xhr.send();
}
