$(document).ready( function () {
    $('#resultsTable').DataTable({order: [[3, 'desc']]});
    $('.deletebutton').click(function(clicked) {
        var to_delete = clicked.target.closest('button').id.split('_')[1];
        console.log('Deleting ' + to_delete);
        $('#deleteModal').modal({
            closable  : true,
            onDeny    : function(){
              return true;
            },
            onApprove : function() {
                $.ajax({
                    type: "POST",
                    url: '/delete_report/' + to_delete,  // We get this from loading.html, which comes from flask
                    dataType: 'json',
                    success: function(data, status, request) {
                        console.log(data);
                        if (data.status === 'error') {
                            $('#errorMsg').text(data.content);
                            $('#errorPopup').show();
                        }
                        else {
                            window.location.reload();
                        }

                    },
                    error: function(xhr, error){
                    }
                })
            }
          }).modal('show');
    });
} );
