$(document).ready(function() {

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
            console.log(data);
            window.location.href = data.results_url;
        },
        error: function(xhr, textStatus, errorThrown){
            $('#errorMsg').text(xhr.status + ' ' + textStatus + ' ' + errorThrown);
            $('#errorPopup').show();
        }
    })
}
