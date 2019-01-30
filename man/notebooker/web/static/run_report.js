$(document).ready(function() {

    $('#run-check-form').submit(function (e) {
        let form = $(this);
        $('#runCheck').removeClass('active').addClass('disabled');
        $('.message').hide();
        $('#parametersDimmer').show();
        var reportName = $('input[name="report_name"]').val();
        $.ajax({
            type: "POST",
            url: "/run_report/" + reportName,
            data: form.serialize(),
            success: function (data, status, request) {
                if (data.status === "Failed") {
                    $('#errorMsg').text(data.content);
                    $('#errorPopup').show();
                    $('#runCheck').removeClass('disabled').addClass('active');
                    $('#parametersDimmer').hide();
                }
                else {
                    console.log(data, status, request);
                    window.location.href = '/results/' + reportName + '/' + data.id;
                }
            },
            error: function(jqXHR, textStatus, errorThrown){
                $('#errorMsg').text(jqXHR.status + ' ' + textStatus + ' ' + errorThrown);
                $('#errorPopup').show();
                // $('#errorMsg');
                console.log(jqXHR);
                console.log(textStatus);
                console.log(errorThrown);
                $('#runCheck').removeClass('disabled').addClass('active');
                $('#parametersDimmer').hide();
            }
        });
        return false;
    });
    $('.message .close')
      .on('click', function() {
        $(this)
          .closest('.message')
          .hide()
        ;
      });
});
