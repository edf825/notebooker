def unit = {
  pyTest 'tests/unit'
}

def integration = {
  pyTest 'tests/integration', "-m 'not compress and not serial'"
}

def latex = {
  sh 'yum install texlive-xetex texlive-xetex-def texlive-adjustbox texlive-upquote texlive-ulem && yum clean all'
}

env.PYTHONPATH = '.:..'  // So that the src directory can be used as a dir

acquireDotsDb(true, "trd-pool", true, 'twsadmin') {
    ahlPython {
        buildLabel='ts2-el7'
        labels='ts2-el7'
        buildPinnedEgg = true
		dockerPath = 'docker'
        medusaVersions = ["27-3"]
        publicProject = true

        preBuildStages = [
            [name: 'LaTeX install',
             body: latex
             ]
        ]

        testStages =  [
            [name: 'Unit',
             body: unit,
            ],
            [name: 'Integration',
             body: integration,
             workers: 1
            ],
        ]
    }
}
