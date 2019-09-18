$(document).ready(function() {
    // Check auth is enabled on our host
    $.ajax({
        url: "/oauth/health",
        success: () => {
            // If enabled, then fetch our login status
            $.ajax({
                url: "/core/user_profile",
                dataType: "json",
                success: (result) => {
                    console.log(result);
                    var user = result;
                    if (result.username) {
                        $('#usernameInfo').text(result.username);
                        $('.loggedIn').fadeIn()
                    } else {
                        $('.loggedOut').fadeIn()
                    }
                },
                error: (jqXHR, textStatus, errorThrown) => {
                    var user = undefined;
                    $('.loggedOut').fadeIn()
                }
            });
        },
        error: () => {
            $('#authArea').hide()
        }
    });

    let sb = $('.ui.left.sidebar');
    sb.sidebar({
    transition: 'overlay'
    });
    sb.sidebar('attach events', '#runReport');
    $('.ui .dropdown').dropdown();

    $('.message .close')
      .on('click', function() {
        $(this)
          .closest('.message')
          .hide()
        ;
      });

});

function rerunReport(jobId, rerunUrl) {
    $.ajax({
        type: 'POST',
        url: rerunUrl,
        dataType: 'json',
        success: function(data, status, request) {
            window.location.href = data.results_url;
        },
        error: function(xhr, textStatus, errorThrown){
            $('#errorMsg').text(xhr.status + ' ' + textStatus + ' ' + errorThrown);
            $('#errorPopup').show();
        }
    })
}

function cloneReport(cloneUrl) {
    window.location.href = cloneUrl;
}
