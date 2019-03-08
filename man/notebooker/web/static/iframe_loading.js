var iframeLoaded = function(iframe) {
    $(".iframeToLoad").show();
    setInterval(function(){
            iframe.style.height = iframe.contentWindow.document.body.scrollHeight + 'px';
            iframe.style.width  = iframe.contentWindow.document.body.scrollWidth  + 'px';
    }, 1);
    $('.iframeLoadingDimmer').removeClass('active').addClass('disabled');

};
