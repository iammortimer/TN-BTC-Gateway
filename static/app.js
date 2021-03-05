function checkETHAddress(event) {
    event.preventDefault();
    tnAddress = document.getElementById("tnAddress").value;

    fetch('/tnAddress/' + tnAddress).then(function(response) {
        response.json().then(function(result) {
            if (result.address == null) {
                alert('No tunnel defined for target address: ' + result.targetAddress);
            } else {
                alert('Tunnel already established from ' + result.address + ' to ' + result.targetAddress + '');
                document.getElementById("ethAddress").value = result.address;
                document.getElementById("ethAddress").readonly = false
            }
        });
    });
}

function establishTunnel(event) {
    event.preventDefault();
    ethAddress = document.getElementById("ethAddress").value;
    tnAddress = document.getElementById("tnAddress").value;

    fetch('/tunnel/' + tnAddress).then(function(response) {
        response.json().then(function(result) {
            if (result.successful == 1) {
                alert('Tunnel successfully established!');
                document.getElementById("ethAddress").value = result.address;
                document.getElementById("ethAddress").readonly = false
            } else if (result.successful == 2) {
                alert('Tunnel already established!');
                document.getElementById("ethAddress").value = result.address;
                document.getElementById("ethAddress").readonly = false
            } else if (result.successful == 3) {
                alert('Tunnel already established!');
                document.getElementById("ethAddress").value = result.address;
                document.getElementById("ethAddress").readonly = false
            } else {
                alert('Invalid address!');
            }
        });
    });
}
