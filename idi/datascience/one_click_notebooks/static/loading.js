$(document).ready(function() {
    let results_url = "";
    let load_status = function() {
        if ($('#loadingStatus').text() === "Checks done!") {
            clearInterval(intervalId);
            window.location.href = results_url;
        }
        $.ajax({
            url: loc,  // We get this from loading.html, which comes from flask
            dataType: 'json',
            success: function(data, status, request) {
                console.log(data);
                results_url = data.results_url;
                $('#loadingStatus').text(data.status);
                if (data.status.startsWith('Error')) {
                    $('#exceptionText').text(data.exception_info);
                    clearInterval(intervalId);
                }
            },
            error: function(xhr, error){
                $('h2').text(xhr.responseJSON.status);
                $('.loader').hide();
                console.log(xhr);
                console.log(error);
                clearInterval(intervalId);
            }
        })
    };
    load_status();
    let intervalId = window.setInterval(function() {
        load_status()
    }, 1500);
});
